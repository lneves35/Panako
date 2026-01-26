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
        if (!path.exists()) path.mkdirs();

        env = org.lmdbjava.Env.create()
                .setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB
                .setMaxDbs(2)
                .setMaxReaders(128) 
                .open(new File(folder), 
                    EnvFlags.MDB_NOSYNC, 
                    EnvFlags.MDB_NOMETASYNC, 
                    EnvFlags.MDB_NOTLS, 
                    EnvFlags.MDB_NORDAHEAD);

        // BOTH must use INTEGERKEY for the C# LightningDB calls to work
        fingerprints = env.openDbi("panako_fingerprints", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
        resourceMap = env.openDbi("panako_resource_map", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);

        storeQueue = new ConcurrentHashMap<>();
        deleteQueue = new ConcurrentHashMap<>();
        queryQueue = new ConcurrentHashMap<>();
    }

    @Override
    public void storeMetadata(long resourceId, String url, float duration, int sampleRate) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            // MATCH C#: C# uses BitConverter.GetBytes(long) -> 8 bytes
            ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceId).flip();
            
            byte[] urlBytes = (url != null) ? url.getBytes() : new byte[0];
            // MATCH C# PARSER: float(4) + int(4) + URL
            // Note: Your C# regex parses "audio files" and "seconds", it needs this order:
            ByteBuffer value = ByteBuffer.allocateDirect(urlBytes.length + 8).order(ByteOrder.LITTLE_ENDIAN);
            value.putFloat(duration);   // Matches stats regex for seconds
            value.putInt(sampleRate);    // Matches stats regex for count
            value.put(urlBytes);
            value.flip();
            
            resourceMap.put(txn, key, value);
            txn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processStoreQueue() {
        if (storeQueue.isEmpty()) return;
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (Map.Entry<Long, List<long[]>> entry : storeQueue.entrySet()) {
                long resourceId = entry.getKey();
                for (long[] data : entry.getValue()) {
                    // KEY: 4-byte hash (int)
                    ByteBuffer key = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
                    key.putInt((int) data[0]).flip();
                    
                    // VALUE: resourceId(8) + timestamp(4)
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
    public void processQueryQueue(Map<Long, List<PanakoHit>> hits, int windowSize, Set<Integer> excludedResources) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            for (Long hash : queryQueue.keySet()) {
                ByteBuffer key = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
                key.putInt(hash.intValue()).flip();
                
                try (Cursor<ByteBuffer> cursor = fingerprints.openCursor(txn)) {
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
                long totalFingerprints = 0;
                long count = 0;
                
                if (c.first()) {
                    do {
                        ByteBuffer v = c.val().order(ByteOrder.LITTLE_ENDIAN);
                        v.rewind();
                        totalDur += v.getFloat();
                        totalFingerprints += v.getInt();
                        count++;
                    } while (c.next());
                }

                // EXACT FORMAT FOR C# REGEX PARSER:
                System.out.println("[MDB INDEX TOTALS]");
                System.out.println("=========================");
                System.out.printf("> %d audio files\n", count);
                System.out.printf("> %.3f seconds of audio\n", totalDur);
                System.out.printf("> %d fingerprint hashes\n", totalFingerprints);
                System.out.println("=========================\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Required Interface methods
    public PanakoResourceMetadata getMetadata(long id) { return null; }
    public void addToStoreQueue(long id, int h, int t, int m) { 
        storeQueue.computeIfAbsent(id, k -> new ArrayList<>()).add(new long[]{h, t, m}); 
    }
    public void addToDeleteQueue(long id, int h, int t, int m) {}
    public void processDeleteQueue() {}
    public void addToQueryQueue(long h) { queryQueue.put(h, new ArrayList<>()); }
    public void processQueryQueue(Map<Long, List<PanakoHit>> h, int w) { processQueryQueue(h, w, null); }
    public void deleteMetadata(long id) {}
    public void clear() {}
    public void close() { if(env != null) env.close(); }
}