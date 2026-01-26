package be.panako.strategy.panako.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.GetOp;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;

import be.panako.cli.Application;

public class PanakoStorageKV implements PanakoStorage {

    private static PanakoStorageKV instance;
    private static final Object mutex = new Object();

    final Dbi<ByteBuffer> fingerprints;
    final Dbi<ByteBuffer> resourceMap;
    final Env<ByteBuffer> env;

    final Map<Long, List<long[]>> storeQueue;
    final Map<Long, List<long[]>> deleteQueue;
    final Map<Long, List<Long>> queryQueue;

    public static synchronized PanakoStorageKV getInstance() {
        if (instance == null) {
            synchronized (mutex) {
                if (instance == null) {
                    instance = new PanakoStorageKV();
                }
            }
        }
        return instance;
    }

    public PanakoStorageKV() {
        String folder = "panako_db"; 
        File path = new File(folder);
        if (!path.exists()) {
            path.mkdirs();
        }

        // HARDWARE OPTIMIZED: NOSYNC for speed, NORDAHEAD to protect RAM
        env = org.lmdbjava.Env.create()
                .setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB
                .setMaxDbs(2)
                .setMaxReaders(128) 
                .open(new File(folder), 
                    EnvFlags.MDB_NOSYNC, 
                    EnvFlags.MDB_NOMETASYNC, 
                    EnvFlags.MDB_NOTLS, 
                    EnvFlags.MDB_NORDAHEAD);

        // REMOVED MDB_INTEGERKEY to prevent the "Store Issue" caused by index sort mismatch
        // Kept DUPSORT as it is required for your fingerprint logic
        fingerprints = env.openDbi("panako_fingerprints", DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
        resourceMap = env.openDbi("panako_resource_map", DbiFlags.MDB_CREATE);

        storeQueue = new ConcurrentHashMap<>();
        deleteQueue = new ConcurrentHashMap<>();
        queryQueue = new ConcurrentHashMap<>();
    }

    @Override
    public void storeMetadata(long resourceId, String url, float duration, int sampleRate) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceId).flip();
            
            byte[] urlBytes = url != null ? url.getBytes() : new byte[0];
            ByteBuffer value = ByteBuffer.allocateDirect(urlBytes.length + 8).order(ByteOrder.LITTLE_ENDIAN);
            value.putFloat(duration).putInt(sampleRate).put(urlBytes).flip();
            
            resourceMap.put(txn, key, value);
            txn.commit();
        } catch (Exception e) {
            System.err.println("CRITICAL: Metadata Store Failed: " + e.getMessage());
        }
    }

    @Override
    public PanakoResourceMetadata getMetadata(long resourceId) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceId).flip();
            ByteBuffer found = resourceMap.get(txn, key);
            if (found != null) {
                return new PanakoResourceMetadata();
            }
        }
        return null;
    }

    @Override
    public void addToStoreQueue(long resourceId, int hash, int time, int metadata) {
        storeQueue.computeIfAbsent(resourceId, k -> new ArrayList<>()).add(new long[] { hash, time, metadata });
    }

    @Override
    public void processStoreQueue() {
        if (storeQueue.isEmpty()) return;
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (Map.Entry<Long, List<long[]>> entry : storeQueue.entrySet()) {
                long resourceId = entry.getKey();
                for (long[] data : entry.getValue()) {
                    // Using 4-byte hash as key in LITTLE_ENDIAN
                    ByteBuffer key = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
                    key.putInt((int) data[0]).flip();
                    
                    // Value: ResourceId (8) + Timestamp (4) = 12 bytes
                    ByteBuffer value = ByteBuffer.allocateDirect(12).order(ByteOrder.LITTLE_ENDIAN);
                    value.putLong(resourceId).putInt((int) data[1]).flip();
                    
                    fingerprints.put(txn, key, value, PutFlags.MDB_NODUPDATA);
                }
            }
            txn.commit();
        } catch (Exception e) {
            System.err.println("CRITICAL: Fingerprint Store Failed: " + e.getMessage());
            e.printStackTrace();
        }
        storeQueue.clear();
    }

    @Override
    public void addToDeleteQueue(long resourceId, int hash, int time, int metadata) {
        deleteQueue.computeIfAbsent(resourceId, k -> new ArrayList<>()).add(new long[] { hash, time, metadata });
    }

    @Override
    public void processDeleteQueue() { deleteQueue.clear(); }

    @Override
    public void addToQueryQueue(long hash) {
        queryQueue.computeIfAbsent(hash, k -> new ArrayList<>()).add(hash);
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> hits, int windowSize) {
        processQueryQueue(hits, windowSize, null);
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> hits, int windowSize, Set<Integer> excludedResources) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            for (Long hash : queryQueue.keySet()) {
                ByteBuffer key = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
                key.putInt(hash.intValue()).flip();
                
                try (org.lmdbjava.Cursor<ByteBuffer> cursor = fingerprints.openCursor(txn)) {
                    if (cursor.get(key, GetOp.MDB_SET)) {
                        do {
                            ByteBuffer val = cursor.val().order(ByteOrder.LITTLE_ENDIAN);
                            // We MUST read in the same order we wrote
                            long resId = val.getLong();
                            int timestamp = val.getInt();
                            
                            if (excludedResources == null || !excludedResources.contains((int)resId)) {
                                List<PanakoHit> hitList = hits.computeIfAbsent(resId, k -> new ArrayList<>());
                                hitList.add(new PanakoHit(resId, hash, 0L, (long)timestamp, 0L));
                            }
                        } while (cursor.next());
                    }
                }
            }
        }
        queryQueue.clear();
    }

    public void printStatistics(boolean verbose) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (org.lmdbjava.Cursor<ByteBuffer> c = resourceMap.openCursor(txn)) {
                double totalDur = 0;
                long count = 0;
                while (c.next()) {
                    ByteBuffer v = c.val().order(ByteOrder.LITTLE_ENDIAN);
                    float d = v.getFloat();
                    if (!Float.isNaN(d) && d > 0) totalDur += d;
                    count++;
                }
                System.out.printf("Storage Stats: %d files, %.2f total hours indexed.\n", count, totalDur / 3600);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteMetadata(long resourceId) {}
    public void clear() {}
    public void close() { 
        if(env != null) env.close(); 
    }
}