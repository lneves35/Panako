package be.panako.strategy.panako.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;

import be.panako.util.Config;
import be.panako.util.FileUtils;
import be.panako.util.Key;

public class PanakoStorageKV implements PanakoStorage {
    
    private static PanakoStorageKV instance;
    private static final Object mutex = new Object();

    public synchronized static PanakoStorageKV getInstance() {
        if (instance == null) {
            synchronized (mutex) {
                if (instance == null) {
                    instance = new PanakoStorageKV();
                }
            }
        }
        return instance;
    }
    
    final Dbi<ByteBuffer> fingerprints;
    final Dbi<ByteBuffer> resourceMap;
    final Env<ByteBuffer> env;
    
    final Map<Long, List<long[]>> storeQueue = new HashMap<>();
    final Map<Long, List<long[]>> deleteQueue = new HashMap<>();
    final Map<Long, List<Long>> queryQueue = new HashMap<>();

    public PanakoStorageKV() {
        String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
        folder = FileUtils.expandHomeDir(folder);
        
        if(!new File(folder).exists()) {
            FileUtils.mkdirs(folder);
        }
        
        // LMDB Environment Tuned for 18GB+ / 800M+ items
        env = org.lmdbjava.Env.create()
            .setMapSize(2L * 1024L * 1024L * 1024L * 1024L) // 2 TB Map Limit
            .setMaxDbs(10)
            .setMaxReaders(512) 
            .open(new File(folder));
        
        // Match existing MDB_INTEGERKEY structure
        fingerprints = env.openDbi("panako_fingerprints", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
        resourceMap = env.openDbi("panako_resource_map", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
    }

    public void close() {
        env.close();
    }
    
    @Override
    public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprintsCount) {
        final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
        byte[] resourcePathBytes = resourcePath.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer val = ByteBuffer.allocateDirect(resourcePathBytes.length + 16).order(ByteOrder.LITTLE_ENDIAN); 
        
        key.putLong(resourceID).flip();
        val.putFloat(duration);
        val.putInt(fingerprintsCount);
        val.put(resourcePathBytes).flip();
        
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            resourceMap.put(txn, key, val);
            txn.commit();
        } catch (Exception e) {
            System.err.println("STORE FAILED: Metadata for " + resourceID + " - " + e.getMessage());
        }
        processStoreQueue();
    }

    @Override
    public PanakoResourceMetadata getMetadata(long resourceID) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceID).flip();
            final ByteBuffer found = resourceMap.get(txn, key);
            
            if(found != null) {
                found.order(ByteOrder.LITTLE_ENDIAN);
                PanakoResourceMetadata metadata = new PanakoResourceMetadata();
                metadata.duration = found.getFloat();
                metadata.numFingerprints = found.getInt();
                metadata.path = StandardCharsets.UTF_8.decode(found).toString();
                metadata.identifier = (int) resourceID;
                return metadata;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return null;    
    }

    @Override
    public void addToStoreQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
        long[] data = {fingerprintHash, (long)resourceIdentifier, (long)t1, (long)f1};
        storeQueue.computeIfAbsent(Thread.currentThread().getId(), k -> new ArrayList<>()).add(data);
    }

    @Override
    public void processStoreQueue() {
        long threadID = Thread.currentThread().getId();
        List<long[]> queue = storeQueue.get(threadID);
        if (queue == null || queue.isEmpty()) return;
        
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            final ByteBuffer val = ByteBuffer.allocateDirect(12).order(ByteOrder.LITTLE_ENDIAN);
            
            try (Cursor<ByteBuffer> c = fingerprints.openCursor(txn)) {
                for(long[] data : queue) {
                    key.putLong(data[0]).flip();
                    val.putInt((int) data[1]).putInt((int) data[2]).putInt((int) data[3]).flip();
                    c.put(key, val);
                    key.clear();
                    val.clear();
                }
            }
            txn.commit();
            queue.clear();
        } catch (Exception e) {
            System.err.println("STORE QUEUE FAILED: " + e.getMessage());
        }
    }

    @Override
    public void addToDeleteQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
        long[] data = {fingerprintHash, (long)resourceIdentifier, (long)t1, (long)f1};
        deleteQueue.computeIfAbsent(Thread.currentThread().getId(), k -> new ArrayList<>()).add(data);
    }

    @Override
    public void processDeleteQueue() {
        long threadID = Thread.currentThread().getId();
        List<long[]> queue = deleteQueue.get(threadID);
        if (queue == null || queue.isEmpty()) return;

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            final ByteBuffer val = ByteBuffer.allocateDirect(12).order(ByteOrder.LITTLE_ENDIAN);
            try (Cursor<ByteBuffer> c = fingerprints.openCursor(txn)) {
                for(long[] data : queue) {
                    key.putLong(data[0]).flip();
                    val.putInt((int) data[1]).putInt((int) data[2]).putInt((int) data[3]).flip();
                    if(c.get(key, val, SeekOp.MDB_GET_BOTH)) {
                        c.delete();
                    }
                    key.clear();
                    val.clear();
                }
            }
            txn.commit();
            queue.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addToQueryQueue(long queryHash) {
        queryQueue.computeIfAbsent(Thread.currentThread().getId(), k -> new ArrayList<>()).add(queryHash);
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range) {
        processQueryQueue(matchAccumulator, range, new HashSet<>());
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range, Set<Integer> resourcesToAvoid) {
        long threadID = Thread.currentThread().getId();
        List<Long> queue = queryQueue.get(threadID);
        if (queue == null || queue.isEmpty()) return;
        
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (Cursor<ByteBuffer> c = fingerprints.openCursor(txn)) {
                final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
                
                for(long originalKey : queue) {
                    long startKey = originalKey - range;
                    long stopKey = originalKey + range;
                    keyBuffer.putLong(startKey).flip();
                    
                    if(c.get(keyBuffer, GetOp.MDB_SET_RANGE)) {
                        do {
                            long hash = c.key().order(ByteOrder.LITTLE_ENDIAN).getLong();
                            if (hash > stopKey) break;

                            ByteBuffer v = c.val().order(ByteOrder.LITTLE_ENDIAN);
                            int resId = v.getInt();
                            int t = v.getInt();
                            int f = v.getInt();

                            if (!resourcesToAvoid.contains(resId)) {
                                matchAccumulator.computeIfAbsent(originalKey, k -> new ArrayList<>())
                                   .add(new PanakoHit(originalKey, hash, (long)t, (long)resId, (long)f));
                            }
                        } while (c.next());
                    }
                    keyBuffer.clear();
                }
            }
            queue.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void printStatistics(boolean detailedStats) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            if(detailedStats) {
                Stat stats = fingerprints.stat(txn);
                System.out.printf("[MDB INDEX statistics]\n");
                System.out.printf("=========================\n");
                System.out.printf("> Number of items: %d\n", stats.entries);
                System.out.printf("=========================\n\n");
            }

            try (Cursor<ByteBuffer> c = resourceMap.openCursor(txn)) {
                double totalDuration = 0;
                long totalPrints = 0;
                long totalResources = 0;
                while(c.next()) {
                    ByteBuffer v = c.val().order(ByteOrder.LITTLE_ENDIAN);
                    float d = v.getFloat();
                    
                    // Sanity check to filter out corrupt/legacy byte-swap values
                    if (!Float.isNaN(d) && !Float.isInfinite(d) && d > 0 && d < 1000000) {
                        totalDuration += d;
                    }
                    
                    // Force unsigned handling for fingerprint counts
                    totalPrints += (v.getInt() & 0xFFFFFFFFL);
                    totalResources++;
                }
                System.out.printf("[MDB INDEX TOTALS]\n");
                System.out.printf("=========================\n");
                System.out.printf("> %d audio files \n", totalResources);
                System.out.printf("> %.3f seconds of audio\n", totalDuration);
                System.out.printf("> %d fingerprint hashes \n", totalPrints);
                System.out.printf("=========================\n\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteMetadata(long resourceID) {   
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
            key.putLong(resourceID).flip();
            resourceMap.delete(txn, key);
            txn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clear() {
        close();
        String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
        folder = FileUtils.expandHomeDir(folder);
        if (FileUtils.exists(folder)) {
            File[] files = new File(folder).listFiles();
            if (files != null) {
                for (File f : files) FileUtils.rm(f.getAbsolutePath());
            }
        }
    }
}