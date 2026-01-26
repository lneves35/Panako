package be.panako.strategy.panako.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.GetOp;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;

import be.panako.cli.Application;
import be.panako.util.Config;
import be.panako.util.FileUtils;
import be.panako.util.Key;

/**
 * Optimized storage using LMDB for high-speed NVMe access.
 * Merged with original duplicate handling logic and LITTLE_ENDIAN key support.
 */
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
	
	// Thread-safe queues using ConcurrentHashMap to prevent thread collisions
	final Map<Long,List<long[]>> storeQueue;
	final Map<Long,List<long[]>> deleteQueue;
	final Map<Long,List<Long>> queryQueue;

	public PanakoStorageKV() {
		String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
		folder = FileUtils.expandHomeDir(folder);
		
		if(!new File(folder).exists()) {
			FileUtils.mkdirs(folder);
		}
		
		// Optimization: NOSYNC for speed, NORDAHEAD for NVMe random access efficiency
		env = org.lmdbjava.Env.create()
				.setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB
				.setMaxDbs(2)
				.setMaxReaders(Application.availableProcessors())
				.open(new File(folder), 
					EnvFlags.MDB_NOSYNC, 
					EnvFlags.MDB_NOMETASYNC, 
					EnvFlags.MDB_NOTLS, 
					EnvFlags.MDB_NORDAHEAD);
		
		// Use DUPSORT and DUPFIXED for fingerprint hashes (multiple matches per hash)
		fingerprints = env.openDbi("panako_fingerprints", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
		resourceMap = env.openDbi("panako_resource_map", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
		
		storeQueue = new ConcurrentHashMap<>();
		deleteQueue = new ConcurrentHashMap<>();
		queryQueue = new ConcurrentHashMap<>();
	}

	public void close() {
		if(env != null) env.close();
	}
	
	@Override
	public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprintsCount) {
		final ByteBuffer key = ByteBuffer.allocateDirect(8);
		byte[] resourcePathBytes = resourcePath.getBytes(StandardCharsets.UTF_8);
		final ByteBuffer val = ByteBuffer.allocateDirect(resourcePathBytes.length + 16); 
		
		key.putLong(resourceID).flip();
		val.putFloat(duration);
		val.putInt(fingerprintsCount);
		val.put(resourcePathBytes).flip();
		
		resourceMap.put(key, val);
	}

	@Override
	public PanakoResourceMetadata getMetadata(long resourceID) {
		PanakoResourceMetadata metadata = null;
		try (Txn<ByteBuffer> txn = env.txnRead()) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8);
			key.putLong(resourceID).flip();
			
			final ByteBuffer found = resourceMap.get(txn, key);
			if(found != null) {
				metadata = new PanakoResourceMetadata();
				metadata.duration = found.getFloat();
				metadata.numFingerprints = found.getInt();
				metadata.path = StandardCharsets.UTF_8.decode(found).toString();
				metadata.identifier = (int) resourceID;
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return metadata;
	}

	@Override
	public void addToStoreQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
		long[] data = {fingerprintHash, resourceIdentifier, t1, f1};
		long threadID = Thread.currentThread().getId();
		storeQueue.computeIfAbsent(threadID, k -> new ArrayList<>()).add(data);
	}

	@Override
	public void processStoreQueue() {
		long threadID = Thread.currentThread().getId();
		List<long[]> queue = storeQueue.get(threadID);
		if (queue == null || queue.isEmpty()) return;
		
		try (Txn<ByteBuffer> txn = env.txnWrite()) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
			final ByteBuffer val = ByteBuffer.allocateDirect(12);
			
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		queue.clear();
	}

	@Override
	public void addToDeleteQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
		long[] data = {fingerprintHash, resourceIdentifier, t1, f1};
		long threadID = Thread.currentThread().getId();
		deleteQueue.computeIfAbsent(threadID, k -> new ArrayList<>()).add(data);
	}

	@Override
	public void processDeleteQueue() {
		long threadID = Thread.currentThread().getId();
		List<long[]> queue = deleteQueue.get(threadID);
		if (queue == null || queue.isEmpty()) return;
		
		try (Txn<ByteBuffer> txn = env.txnWrite()) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
			final ByteBuffer val = ByteBuffer.allocateDirect(12);
			
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		queue.clear();
	}

	@Override
	public void addToQueryQueue(long queryHash) {
		long threadID = Thread.currentThread().getId();
		queryQueue.computeIfAbsent(threadID, k -> new ArrayList<>()).add(queryHash);
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
					
					// MDB_SET_RANGE finds the first key >= startKey
					if(c.get(keyBuffer, GetOp.MDB_SET_RANGE)) {
						while (true) {
							long hash = c.key().order(ByteOrder.LITTLE_ENDIAN).getLong();
							if (hash > stopKey) break;

							ByteBuffer val = c.val();
							int resID = val.getInt();
							int t = val.getInt();
							int f = val.getInt();

							if(!resourcesToAvoid.contains(resID)) {
								matchAccumulator.computeIfAbsent(originalKey, k -> new ArrayList<>())
										.add(new PanakoHit(originalKey, hash, t, (long)resID, f));
							}
							
							// Correct progression for DUPSORT: NEXT moves through duplicates AND next keys
							if (!c.seek(SeekOp.MDB_NEXT)) break;
						}
					}
					keyBuffer.clear();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		queue.clear();
	}

	@Override
	public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range) {
		processQueryQueue(matchAccumulator, range, new HashSet<>());
	}

	@Override
	public void printStatistics(boolean detailedStats) {
		try (Txn<ByteBuffer> txn = env.txnRead()) {
			Stat stats = fingerprints.stat(txn);
			if(detailedStats) {
				System.out.println("[MDB INDEX statistics]");
				System.out.printf("> Number of items in databases: %d\n", stats.entries);
			}
			
			try (Cursor<ByteBuffer> c = resourceMap.openCursor(txn)) {
				double totalDuration = 0;
				long totalPrints = 0;
				long totalResources = 0;
				while(c.seek(SeekOp.MDB_NEXT)) {
					totalDuration += c.val().getFloat();
					totalPrints += c.val().getInt();
					totalResources++;
				}
				System.out.println("[MDB INDEX TOTALS]");
				System.out.printf("> %d audio files \n> %.3f seconds of audio\n> %d fingerprint hashes \n", 
						totalResources, totalDuration, totalPrints);
			}
		}
	}

	@Override
	public void deleteMetadata(long resourceID) {	
		try (Txn<ByteBuffer> txn = env.txnWrite()) {
			ByteBuffer key = ByteBuffer.allocateDirect(8);
			key.putLong(resourceID).flip();
			resourceMap.delete(txn, key);
			txn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void clear() {
		close();
		String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
		folder = FileUtils.expandHomeDir(folder);
		if(FileUtils.exists(folder)) {
			for(File f : new File(folder).listFiles()) {
				FileUtils.rm(f.getAbsolutePath());
			}
		}
	}
}