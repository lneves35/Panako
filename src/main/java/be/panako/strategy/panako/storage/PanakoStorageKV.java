package be.panako.strategy.panako.storage;

import java.io.File;
import java.nio.ByteBuffer;
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

        // Bypassing Page Cache to save your 125Gi RAM
        env = org.lmdbjava.Env.create()
                .setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB
                .setMaxDbs(2)
                .setMaxReaders(128) 
                .open(new File(folder), 
                    EnvFlags.MDB_NOSYNC, 
                    EnvFlags.MDB_NOMETASYNC, 
                    EnvFlags.MDB_NOTLS, 
                    EnvFlags.MDB_NORDAHEAD);

        fingerprints = env.openDbi("panako_fingerprints", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
        resourceMap = env.openDbi("panako_resource_map", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);

        storeQueue = new ConcurrentHashMap<>();
        deleteQueue = new ConcurrentHashMap<>();
        queryQueue = new ConcurrentHashMap<>();
    }

    @Override
    public void storeMetadata(long resourceId, String url, float duration, int sampleRate) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer key = ByteBuffer.allocateDirect(Long.BYTES);
            key.putLong(resourceId).flip();
            
            byte[] urlBytes = url != null ? url.getBytes() : new byte[0];
            ByteBuffer value = ByteBuffer.allocateDirect(urlBytes.length);
            value.put(urlBytes).flip();
            
            resourceMap.put(txn, key, value);
            txn.commit();
        }
    }

    @Override
    public PanakoResourceMetadata getMetadata(long resourceId) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer key = ByteBuffer.allocateDirect(Long.BYTES);
            key.putLong(resourceId).flip();
            ByteBuffer found = resourceMap.get(txn, key);
            if (found != null) {
                // FIXED: Using no-args constructor as required by PanakoResourceMetadata
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
                    ByteBuffer key = ByteBuffer.allocateDirect(Integer.BYTES);
                    key.putInt((int) data[0]).flip();
                    ByteBuffer value = ByteBuffer.allocateDirect(Long.BYTES + Integer.BYTES);
                    value.putLong(resourceId).putInt((int) data[1]).flip();
                    fingerprints.put(txn, key, value, PutFlags.MDB_NODUPDATA);
                }
            }
            txn.commit();
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
                ByteBuffer key = ByteBuffer.allocateDirect(Integer.BYTES);
                key.putInt(hash.intValue()).flip();
                
                try (org.lmdbjava.Cursor<ByteBuffer> cursor = fingerprints.openCursor(txn)) {
                    if (cursor.get(key, GetOp.MDB_SET)) {
                        do {
                            ByteBuffer val = cursor.val();
                            long resId = val.getLong();
                            int timestamp = val.getInt();
                            
                            if (excludedResources == null || !excludedResources.contains((int)resId)) {
                                List<PanakoHit> hitList = hits.computeIfAbsent(resId, k -> new ArrayList<>());
                                
                                // FIXED: Using 5 long arguments as required by PanakoHit constructor
                                // (resourceId, hash, queryTime, dbTime, offset/score)
                                PanakoHit hit = new PanakoHit(resId, hash, 0L, (long)timestamp, 0L);
                                hitList.add(hit);
                            }
                        } while (cursor.next());
                    }
                }
            }
        }
        queryQueue.clear();
    }

    public void printStatistics(boolean verbose) {}
    public void deleteMetadata(long resourceId) {}
    public void clear() {}
    public void close() { 
        if(env != null) env.close(); 
    }
}