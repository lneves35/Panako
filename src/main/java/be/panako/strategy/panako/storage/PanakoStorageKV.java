/***************************************************************************
* *
* Panako - acoustic fingerprinting                                          *
* Copyright (C) 2014 - 2022 - Joren Six / IPEM                              *
* *
* This program is free software: you can redistribute it and/or modify      *
* it under the terms of the GNU Affero General Public License as            *
* published by the Free Software Foundation, either version 3 of the        *
* License, or (at your option) any later version.                           *
* *
* This program is distributed in the hope that it will be useful,           *
* but WITHOUT ANY WARRANTY; without even the implied warranty of            *
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the             *
* GNU Affero General Public License for more details.                       *
* *
* You should have received a copy of the GNU Affero General Public License *
* along with this program.  If not, see <http://www.gnu.org/licenses/>      *
* *
****************************************************************************
* ______   _______   ___   __   ________   ___   ___   ______          *
* /_____/\ /_______/\ /__/\ /__/\ /_______/\ /___/\/__/\ /_____/\         *
* \:::_ \ \\::: _  \ \\::\_\\  \ \\::: _  \ \\::.\ \\ \ \\:::_ \ \        *
* \:(_) \ \\::(_)  \ \\:. `-\  \ \\::(_)  \ \\:: \/_) \ \\:\ \ \ \       *
* \: ___\/ \:: __  \ \\:. _   \ \\:: __  \ \\:. __  ( ( \:\ \ \ \      *
* \ \ \    \:.\ \  \ \\. \`-\  \ \\:.\ \  \ \\: \ )  \ \ \:\_\ \ \     *
* \_\/     \__\/\__\/ \__\/ \__\/ \__\/\__\/ \__\/\__\/  \_____\/     *
* *
****************************************************************************
* *
* Panako                                      *
* Acoustic Fingerprinting                            *
* *
****************************************************************************/

package be.panako.strategy.panako.storage;

import java.io.File;
import java.nio.ByteBuffer;
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
 * A storage in a key value store optimized for node1 NVMe hardware.
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
	
	// Thread-safe queues for concurrent processing
	final Map<Long,List<long[]>> storeQueue;
	final Map<Long,List<long[]>> deleteQueue;
	final Map<Long,List<Long>> queryQueue;

	public PanakoStorageKV() {
		String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
		folder = FileUtils.expandHomeDir(folder);
		
		if(!new File(folder).exists()) {
			FileUtils.mkdirs(folder);
		}
		if(!new File(folder).exists()) {
			throw new RuntimeException("Could not create LMDB folder: " + folder);
		}
		
		// MERGED: Hardware optimizations (No readahead, No sync for speed)
		env = org.lmdbjava.Env.create()
				.setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB
				.setMaxDbs(2)
				.setMaxReaders(Application.availableProcessors())
				.open(new File(folder), 
					EnvFlags.MDB_NOSYNC, 
					EnvFlags.MDB_NOMETASYNC, 
					EnvFlags.MDB_NOTLS, 
					EnvFlags.MDB_NORDAHEAD);
		
		final String fingerprintName = "panako_fingerprints";
		fingerprints = env.openDbi(fingerprintName, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
		
		final String resourceName = "panako_resource_map";		
		resourceMap = env.openDbi(resourceName, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
		
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
				// Use the buffer directly as it's positioned after 'found'
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
			final ByteBuffer key = ByteBuffer.allocateDirect(8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
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
			final ByteBuffer key = ByteBuffer.allocateDirect(8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
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
				final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
				
				for(long originalKey : queue) {
					long startKey = originalKey - range;
					long stopKey = originalKey + range;
					keyBuffer.putLong(startKey).flip();
					
					if(c.get(keyBuffer, GetOp.MDB_SET_RANGE)) {
						do {
							long hash = c.key().order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
							if (hash > stopKey) break;

							int resID = c.val().getInt();
							int t = c.val().getInt();
							int f = c.val().getInt();

							if(!resourcesToAvoid.contains(resID)) {
								matchAccumulator.computeIfAbsent(originalKey, k -> new ArrayList<>())
										.add(new PanakoHit(originalKey, hash, t, resID, f));
							}
						} while (c.seek(SeekOp.MDB_NEXT));
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
	public void printStatistics(boolean detailedStats) {
		try (Txn<ByteBuffer> txn = env.txnRead()) {
			Stat stats = fingerprints.stat(txn);
			if(detailedStats) {
				String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
				String dbpath = FileUtils.combine(folder, "data.mdb");
				long dbSizeInMB = new File(dbpath).length() / (1024 * 1024);
				
				System.out.println("[MDB INDEX statistics]");
				System.out.println("=========================");
				System.out.printf("> Size of database page:        %d\n", stats.pageSize);
				System.out.printf("> Depth of the B-tree:          %d\n", stats.depth);
				System.out.printf("> Number of items in databases: %d\n", stats.entries);
				System.out.printf("> File size of the databases:   %dMB\n", dbSizeInMB);
				System.out.println("=========================\n");
			}
			
			try (Cursor<ByteBuffer> c = resourceMap.openCursor(txn)) {
				double totalDuration = 0;
				long totalPrints = 0;
				long totalResources = 0;
				
				while(c.seek(SeekOp.MDB_NEXT)) {
					float duration = c.val().getFloat();
					int numFingerprints = c.val().getInt();
					totalDuration += duration;
					totalPrints += numFingerprints;
					totalResources++;
				}
				
				System.out.println("[MDB INDEX TOTALS]");
				System.out.println("=========================");
				System.out.printf("> %d audio files \n", totalResources);
				System.out.printf("> %.3f seconds of audio\n", totalDuration);
				System.out.printf("> %d fingerprint hashes \n", totalPrints);
				System.out.println("=========================\n");
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