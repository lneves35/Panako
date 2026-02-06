/***************************************************************************
* *
* Panako - acoustic fingerprinting                                         *
* Copyright (C) 2014 - 2022 - Joren Six / IPEM                             *
* *
* This program is free software: you can redistribute it and/or modify     *
* it under the terms of the GNU Affero General Public License as           *
* published by the Free Software Foundation, either version 3 of the       *
* License, or (at your option) any later version.                          *
* *
* This program is distributed in the hope that it will be useful,          *
* but WITHOUT ANY WARRANTY; without even the implied warranty of           *
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            *
* GNU Affero General Public License for more details.                      *
* *
* You should have received a copy of the GNU Affero General Public License *
* along with this program.  If not, see <http://www.gnu.org/licenses/>     *
* *
****************************************************************************/

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
 * A storage in a key value store
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
				
		env = org.lmdbjava.Env.create()
			.setMapSize(1024L * 1024L * 1024L * 1024L) // 1 TB
			.setMaxDbs(2)
			.setMaxReaders(Application.availableProcessors())
			.open(new File(folder), 
				EnvFlags.MDB_NOSYNC, 
				EnvFlags.MDB_NOMETASYNC, 
				EnvFlags.MDB_NOTLS, 
				EnvFlags.MDB_NORDAHEAD,
				EnvFlags.MDB_NOSUBDIR,
				EnvFlags.MDB_WRITEMAP,
				EnvFlags.MDB_MAPASYNC
			);
		
		fingerprints = env.openDbi("panako_fingerprints", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
		resourceMap = env.openDbi("panako_resource_map", DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
		
		storeQueue = new HashMap<Long, List<long[]>>();
		deleteQueue = new HashMap<Long, List<long[]>>();
		queryQueue = new HashMap<Long, List<Long>>();
	}

	public void close() {
		env.close();
	}
	
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

	public PanakoResourceMetadata getMetadata(long resourceID) {
		PanakoResourceMetadata metadata = null;
		try (Txn<ByteBuffer> txn = env.txnRead()) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8);
			key.putLong(resourceID).flip();
		    if(resourceMap.get(txn, key) != null) {
		    	metadata = new PanakoResourceMetadata();
		    	final ByteBuffer fetchedVal = txn.val();
		    	metadata.duration = fetchedVal.getFloat();
		    	metadata.numFingerprints = fetchedVal.getInt();
		    	metadata.path = StandardCharsets.UTF_8.decode(fetchedVal).toString();
		    	metadata.identifier = (int) resourceID;
		    }
		} catch(Exception e) { e.printStackTrace(); }
		return metadata;    
	}
	
	public void addToStoreQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
		long threadID = Thread.currentThread().getId();
		if(!storeQueue.containsKey(threadID))
			storeQueue.put(threadID, new ArrayList<long[]>());
		storeQueue.get(threadID).add(new long[]{fingerprintHash, resourceIdentifier, t1, f1});
	}

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
		    	  key.clear(); val.clear();
		      }  
		    }
		    txn.commit();
		    queue.clear();
		} catch (Exception e) { e.printStackTrace(); }
	}

	public void addToDeleteQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
		long threadID = Thread.currentThread().getId();
		if(!deleteQueue.containsKey(threadID))
			deleteQueue.put(threadID, new ArrayList<long[]>());
		deleteQueue.get(threadID).add(new long[]{fingerprintHash, resourceIdentifier, t1, f1});
	}

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
		    	  if(c.get(key, val, SeekOp.MDB_GET_BOTH)) c.delete();
		    	  key.clear(); val.clear();
		      }  
		    }
		    txn.commit();
		    queue.clear();
		} catch (Exception e) { e.printStackTrace(); }
	}

	public void addToQueryQueue(long queryHash) {
		long threadID = Thread.currentThread().getId();
		if(!queryQueue.containsKey(threadID))
			queryQueue.put(threadID, new ArrayList<Long>());
		queryQueue.get(threadID).add(queryHash);
	}

	public void processQueryQueue(Map<Long,List<PanakoHit>> matchAccumulator, int range) {
		processQueryQueue(matchAccumulator, range, new HashSet<Integer>());
	}

	@Override
	public void processQueryQueue(Map<Long,List<PanakoHit>> matchAccumulator, int range, Set<Integer> resourcesToAvoid) {
		long threadID = Thread.currentThread().getId();
		List<Long> queue = queryQueue.get(threadID);
		if (queue == null || queue.isEmpty()) return;
		
		try (Txn<ByteBuffer> txn = env.txnRead()) {
		    try (Cursor<ByteBuffer> c = fingerprints.openCursor(txn)) {
		      final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
		      for(long originalKey : queue) {
		    	  long startKey = originalKey - range;
		    	  long stopKey = originalKey + range;
		    	  keyBuffer.clear();
		    	  keyBuffer.putLong(startKey).flip();
		      
                  // OPTIMIZATION: One pass using MDB_NEXT is much faster for DUPFIXED
                  if(c.get(keyBuffer, GetOp.MDB_SET_RANGE)) {
                      do {
                          long fingerprintHash = c.key().order(ByteOrder.LITTLE_ENDIAN).getLong();
                          if(fingerprintHash > stopKey) break;
                          
                          ByteBuffer v = c.val();
                          int resourceID = v.getInt();
                          int t = v.getInt();
                          int f = v.getInt();
                          
                          if(!resourcesToAvoid.contains(resourceID)) {
                              if(!matchAccumulator.containsKey(originalKey))
                                  matchAccumulator.put(originalKey, new ArrayList<PanakoHit>());
                              matchAccumulator.get(originalKey).add(new PanakoHit(originalKey, fingerprintHash, t, (long)resourceID, f));
                          }
                      } while (c.seek(SeekOp.MDB_NEXT));
                  }
		      }
		    }
		    queue.clear();
		}
	}

	@Override
	public void printStatistics(boolean detailedStats) {
		try (Txn<ByteBuffer> txn = env.txnRead()) {
		    Stat stats = fingerprints.stat(txn);
	        if(detailedStats) {
	    	    String folder = Config.get(Key.PANAKO_LMDB_FOLDER);
	    	    long dbSizeInMB = 0;
	    	    if(folder != null) {
	    		    File dbFile = new File(FileUtils.combine(FileUtils.expandHomeDir(folder), "data.mdb"));
	    		    if(dbFile.exists()) dbSizeInMB = dbFile.length() / (1024 * 1024);
	    	    }
	    	  
		        System.out.println("[MDB INDEX statistics]");
		        System.out.println("=========================");
		        System.out.printf("> Size of database page:        %d\n", stats.pageSize);
		        System.out.printf("> Depth of the B-tree:          %d\n", stats.depth);
		        System.out.printf("> Number of items in databases: %d\n", stats.entries);
		        System.out.printf("> File size of the databases:   %dMB\n", dbSizeInMB);
		        System.out.println("=========================\n");
	        }
	    } catch(Exception e) { e.printStackTrace(); }
	    
	    try (Txn<ByteBuffer> txn = env.txnRead(); Cursor<ByteBuffer> c = resourceMap.openCursor(txn)) {
		      double totalDuration = 0; long totalPrints = 0; long totalResources = 0;
		      while(c.seek(SeekOp.MDB_NEXT)) {
			      ByteBuffer v = c.val();
                  float duration = v.getFloat();
			      int numFingerprints = v.getInt();
			      totalDuration += duration; totalPrints += numFingerprints; totalResources++;
		      }
		      System.out.println("[MDB INDEX TOTALS]");
		      System.out.println("=========================");
		      System.out.printf("> %d audio files\n> %.3f seconds\n> %d hashes\n", totalResources, totalDuration, totalPrints);
		      System.out.println("=========================\n");
	    } catch(Exception e) { e.printStackTrace(); }
	}

	public void deleteMetadata(long resourceID) {	
		try (Txn<ByteBuffer> txn = env.txnWrite()) {
			final ByteBuffer key = ByteBuffer.allocateDirect(8);
			key.putLong(resourceID).flip();
			if(resourceMap.get(txn, key) != null) resourceMap.delete(txn, key);
		    txn.commit();
	    } catch (Exception e) { e.printStackTrace(); }
	}

	public void clear() {
		fingerprints.close();
		resourceMap.close();
		env.close();
		String folder = FileUtils.expandHomeDir(Config.get(Key.PANAKO_LMDB_FOLDER));
		if(folder != null && new File(folder).exists()) {
			for(File f : new File(folder).listFiles()) f.delete();
		}
	}
}