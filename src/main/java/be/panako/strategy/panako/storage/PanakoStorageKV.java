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
import org.lmdbjava.Cursor;

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

        // Optimization for large DB (21GB): NoSync for write speed, NoRdahead to save RAM
        env = org.lmdbjava.Env.create()
                .setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB
                .setMaxDbs(2)
                .setMaxReaders(128) 
                .open(new File(folder), 
                    EnvFlags.MDB_NOSYNC, 
                    EnvFlags.MDB_NOMETASYNC, 
                    EnvFlags.MDB_NOTLS, 
                    EnvFlags.MDB_NORDAHEAD);

        // CRITICAL: MDB_INTEGERKEY must match the data type (int/long)
        fingerprints = env.openDbi("panako_fingerprints", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
        resourceMap = env.openDbi("panako_resource_map", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);

        storeQueue = new ConcurrentHashMap<>();
        deleteQueue = new ConcurrentHashMap<>();
        queryQueue = new ConcurrentHashMap<>();
    }

    @Override
    public void storeMetadata(long resourceId, String url, float duration, int sampleRate) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            // Key MUST be exactly 8 bytes for Long INTEGERKEY
            ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceId).flip();
            
            byte[] urlBytes = (url != null) ? url.getBytes() : new byte[0];
            // Value: float(4) + int(4) + long padding(8) = 16 bytes header
            ByteBuffer value = ByteBuffer.allocateDirect(urlBytes.length + 16).order(ByteOrder.LITTLE_ENDIAN);
            value.putFloat(duration);
            value.putInt(sampleRate);
            value.putLong(0L); 
            value.put(urlBytes);
            value.flip();
            
            resourceMap.put(txn, key, value);
            txn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public PanakoResourceMetadata getMetadata(long resourceId) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceId).flip();
            
            ByteBuffer found = resourceMap.get(txn, key);
            if (found != null) {
                found.order(ByteOrder.LITTLE_ENDIAN);
                PanakoResourceMetadata meta = new PanakoResourceMetadata();
                meta.duration = found.getFloat();
                meta.numFingerprints = found.getInt();
                return meta;
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
                    // Key: 4-byte hash
                    ByteBuffer key = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
                    key.putInt((int) data[0]).flip();
                    
                    // Value: resourceId(8) + timestamp(4)
                    ByteBuffer value = ByteBuffer.allocateDirect(12).order(ByteOrder.LITTLE_ENDIAN);
                    value.putLong(resourceId).putInt((int) data[1]).flip();
                    
                    fingerprints.put(txn, key, value, PutFlags.MDB_NODUPDATA);
                }
            }
            txn.commit();
        } catch (Exception e) {
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
        if (queryQueue.isEmpty()) return;
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            for (Long hash : queryQueue.keySet()) {
                // FIXED: Direct allocation and explicit sizing for MDB_INTEGERKEY query
                ByteBuffer key = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
                key.putInt(hash.intValue()).flip();
                
                try (Cursor<ByteBuffer> cursor = fingerprints.openCursor(txn)) {
                    // MDB_SET finds the exact match for the integer key
                    if (cursor.get(key, GetOp.MDB_SET)) {
                        do {
                            ByteBuffer val = cursor.val().order(ByteOrder.LITTLE_ENDIAN);
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

    @Override
    public void printStatistics(boolean verbose) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (Cursor<ByteBuffer> c = resourceMap.openCursor(txn)) {
                double totalDur = 0;
                long count = 0;
                if (c.first()) {
                    do {
                        ByteBuffer v = c.val().order(ByteOrder.LITTLE_ENDIAN);
                        // Reset buffer position to start of record
                        v.rewind();
                        float d = v.getFloat();
                        if (!Float.isNaN(d) && d > 0) totalDur += d;
                        count++;
                    } while (c.next());
                }
                System.out.println("[MDB INDEX TOTALS]");
                System.out.println("=========================");
                System.out.printf("> %d audio files found\n", count);
                System.out.printf("> %.2f total hours indexed.\n", totalDur / 3600);
                System.out.println("=========================\n");
            }
        } catch (Exception e) {
            System.err.println("Stats Error: " + e.getMessage());
        }
    }

    public void deleteMetadata(long resourceId) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceId).flip();
            resourceMap.delete(txn, key);
            txn.commit();
        }
    }
    
    public void clear() {}
    public void close() { 
        if(env != null) env.close(); 
    }
}