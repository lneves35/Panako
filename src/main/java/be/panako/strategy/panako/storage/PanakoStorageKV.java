/***************************************************************************
* *
* Panako - acoustic fingerprinting                                         *
* Copyright (C) 2014 - 2026 - Joren Six / IPEM                             *
* *
* This program is free software: you can redistribute it and/or modify      *
* it under the terms of the GNU Affero General Public License as          *
* published by the Free Software Foundation, either version 3 of the      *
* License, or (at your option) any later version.                         *
* *
* This program is distributed in the hope that it will be useful,         *
* but WITHOUT ANY WARRANTY; without even the implied warranty of          *
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the          *
* GNU Affero General Public License for more details.                     *
* *
* You should have received a copy of the GNU Affero General Public License *
* along with this program.  If not, see <http://www.gnu.org/licenses/>      *
* *
****************************************************************************
* ______   ________   ___   __   ________   ___   ___   ______          *
* /_____/\ /_______/\ /__/\ /__/\ /_______/\ /___/\/__/\ /_____/\         *
* \:::_ \ \\::: _  \ \\::\_\\  \ \\::: _  \ \\::.\ \\ \ \\:::_ \ \        *
* \:(_) \ \\::(_)  \ \\:. `-\  \ \\::(_)  \ \\:: \/_) \ \\:\ \ \ \       *
* \: ___\/ \:: __  \ \\:. _   \ \\:: __  \ \\:. __  ( ( \:\ \ \ \      *
* \ \ \    \:.\ \  \ \\. \`-\  \ \\:.\ \  \ \\: \ )  \ \ \:\_\ \ \     *
* \_\/     \__\/\__\/ \__\/ \__\/ \__\/\__\/ \__\/\__\/  \_____\/     *
* *
****************************************************************************
* *
* Panako                                                                  *
* Acoustic Fingerprinting                                                 *
* *
****************************************************************************/

package be.panako.strategy.panako.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * A storage in a key value store optimized for linear disk access.
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

	final Map<Long, List<long[]>> storeQueue;
	final Map<Long, List<long[]>> deleteQueue;
	final Map<Long, List<Long>> queryQueue;

	public PanakoStorageKV() {
		String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
		folder = FileUtils.expandHomeDir(folder);

		if (!new File(folder).exists()) {
			FileUtils.mkdirs(folder);
		}
		if (!new File(folder).exists()) {
			throw new RuntimeException("Could not create LMDB folder: " + folder);
		}

		env = org.lmdbjava.Env.create()
				.setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB max!
				.setMaxDbs(2)
				.setMaxReaders(Application.availableProcessors())
				.open(new File(folder), 
					EnvFlags.MDB_NOSYNC, 
					EnvFlags.MDB_NOMETASYNC, 
					EnvFlags.MDB_NOTLS, 
					EnvFlags.MDB_NORDAHEAD,
					EnvFlags.MDB_WRITEMAP, 
					EnvFlags.MDB_MAPASYNC
				);

		final String fingerprintName = "panako_fingerprints";
		fingerprints = env.openDbi(fingerprintName, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);

		final String resourceName = "panako_resource_map";
		resourceMap = env.openDbi(resourceName, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);

		storeQueue = new HashMap<>();
		deleteQueue = new HashMap<>();
		queryQueue = new HashMap<>();
	}

	public void close() {
		env.close();
	}

	public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprintsCount) {
		final ByteBuffer key = ByteBuffer.allocateDirect(8);
		byte[] pathBytes = resourcePath.getBytes(StandardCharsets.UTF_8);
		final ByteBuffer val = ByteBuffer.allocateDirect(pathBytes.length + 16);
		key.putLong(resourceID).flip();
		val.putFloat(duration).putInt(fingerprintsCount).put(pathBytes).flip();
		resourceMap.put(key, val);
	}

	public PanakoResourceMetadata getMetadata(long resourceID) {
		try (Txn<ByteBuffer> txn = env.txnRead()) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8);
			key.putLong(resourceID).flip();
			final ByteBuffer found = resourceMap.get(txn, key);
			if (found != null) {
				PanakoResourceMetadata metadata = new PanakoResourceMetadata();
				final ByteBuffer fetchedVal = txn.val();
				metadata.duration = fetchedVal.getFloat();
				metadata.numFingerprints = fetchedVal.getInt();
				metadata.path = StandardCharsets.UTF_8.decode(fetchedVal).toString();
				metadata.identifier = (int) resourceID;
				return metadata;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void addToStoreQueue(long hash, int id, int t1, int f1) {
		queryQueue.computeIfAbsent(Thread.currentThread().getId(), k -> new ArrayList<>()); // Fix: simplified map logic
		storeQueue.computeIfAbsent(Thread.currentThread().getId(), k -> new ArrayList<>()).add(new long[]{hash, id, t1, f1});
	}

	public void processStoreQueue() {
		List<long[]> queue = storeQueue.get(Thread.currentThread().getId());
		if (queue == null || queue.isEmpty()) return;

		try (Txn<ByteBuffer> txn = env.txnWrite(); Cursor<ByteBuffer> c = fingerprints.openCursor(txn)) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
			final ByteBuffer val = ByteBuffer.allocateDirect(12);
			for (long[] data : queue) {
				key.putLong(data[0]).flip();
				val.putInt((int) data[1]).putInt((int) data[2]).putInt((int) data[3]).flip();
				c.put(key, val);
				key.clear();
				val.clear();
			}
			txn.commit();
			queue.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void addToDeleteQueue(long hash, int id, int t1, int f1) {
		deleteQueue.computeIfAbsent(Thread.currentThread().getId(), k -> new ArrayList<>()).add(new long[]{hash, id, t1, f1});
	}

	@Override
	public void processDeleteQueue() {
		List<long[]> queue = deleteQueue.get(Thread.currentThread().getId());
		if (queue == null || queue.isEmpty()) return;

		try (Txn<ByteBuffer> txn = env.txnWrite(); Cursor<ByteBuffer> c = fingerprints.openCursor(txn)) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
			final ByteBuffer val = ByteBuffer.allocateDirect(12);
			for (long[] data : queue) {
				key.putLong(data[0]).flip();
				val.putInt((int) data[1]).putInt((int) data[2]).putInt((int) data[3]).flip();
				if (c.get(key, val, SeekOp.MDB_GET_BOTH)) {
					c.delete();
				}
				key.clear();
				val.clear();
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
		List<Long> queue = queryQueue.get(Thread.currentThread().getId());
		if (queue == null || queue.isEmpty()) return;

		// CRITICAL PERFORMANCE CHANGE:
		// Sorting the query hashes ensures we traverse the 1TB data file linearly.
		// This minimizes random I/O and makes the OS page cache much more efficient.
		Collections.sort(queue);

		try (Txn<ByteBuffer> txn = env.txnRead(); Cursor<ByteBuffer> c = fingerprints.openCursor(txn)) {
			final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);

			for (long originalKey : queue) {
				long startKey = originalKey - range;
				long stopKey = originalKey + range;

				keyBuffer.putLong(startKey).flip();

				// Seek to the first key >= startKey
				if (c.get(keyBuffer, GetOp.MDB_SET_RANGE)) {
					while (true) {
						long foundHash = c.key().order(ByteOrder.LITTLE_ENDIAN).getLong();

						// If we are past the stopKey, move to the next query
						if (foundHash > stopKey) break;

						// Use MDB_NEXT_DUP to iterate all resources for this specific hash efficiently
						do {
							ByteBuffer v = c.val();
							int resourceID = v.getInt();
							int t = v.getInt();
							int f = v.getInt();

							if (!resourcesToAvoid.contains(resourceID)) {
								matchAccumulator.computeIfAbsent(originalKey, k -> new ArrayList<>())
										.add(new PanakoHit(originalKey, foundHash, t, resourceID, f));
							}
						} while (c.seek(SeekOp.MDB_NEXT_DUP));

						// Move to the next unique key in the index
						if (!c.seek(SeekOp.MDB_NEXT)) break;
					}
				}
				keyBuffer.clear();
			}
			queue.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void printStatistics(boolean detailedStats) {
		try (Txn<ByteBuffer> txn = env.txnRead()) {
			Stat stats = fingerprints.stat(txn);
			if (detailedStats) {
				String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
				long dbSizeInMB = new File(FileUtils.combine(folder, "data.mdb")).length() / (1024 * 1024);
				System.out.println("[MDB INDEX statistics]\n=========================");
				System.out.printf("> Page size: %d\n> B-tree depth: %d\n> Total entries: %d\n> DB size: %dMB\n", 
						stats.pageSize, stats.depth, stats.entries, dbSizeInMB);
			}

			try (Cursor<ByteBuffer> c = resourceMap.openCursor(txn)) {
				double totalDur = 0; long totalFP = 0; long totalRes = 0;
				double maxPPS = 0, minPPS = 100000;
				String maxPath = "", minPath = "";

				while (c.seek(SeekOp.MDB_NEXT)) {
					float d = c.val().getFloat();
					int n = c.val().getInt();
					float pps = n / d;
					String p = StandardCharsets.UTF_8.decode(c.val()).toString();
					if (pps > maxPPS) { maxPPS = pps; maxPath = p; }
					if (pps < minPPS) { minPPS = pps; minPath = p; }
					totalDur += d; totalFP += n; totalRes++;
				}
				System.out.printf("[PANAKO version 2026.2.6.1]\n[MDB INDEX TOTALS]\n=========================\n");
				System.out.printf("> %d audio files\n> %.2f seconds of audio\n> %d hashes\n", totalRes, totalDur, totalFP);
				System.out.printf("> Avg prints/s: %.1f\n=========================\n", totalFP / totalDur);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void deleteMetadata(long resourceID) {
		try (Txn<ByteBuffer> txn = env.txnWrite()) {
			ByteBuffer key = ByteBuffer.allocateDirect(8);
			key.putLong(resourceID).flip();
			if (resourceMap.get(txn, key) != null) {
				resourceMap.delete(txn, key);
			}
			txn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void clear() {
		fingerprints.close();
		resourceMap.close();
		env.close();
		String folder = FileUtils.expandHomeDir(Config.get(Key.PANAKO_LMDB_FOLDER));
		File[] files = new File(folder).listFiles();
		if (files != null) {
			for (File f : files) {
				System.out.println("Removed " + f.getAbsolutePath());
				FileUtils.rm(f.getAbsolutePath());
			}
		}
	}
}