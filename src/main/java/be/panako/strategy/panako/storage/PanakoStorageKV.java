@ -113,30 +113,30 @@ public class PanakoStorageKV implements PanakoStorage{
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
		
		final String fingerprintName = "panako_fingerprints";
		fingerprints = env.openDbi(fingerprintName, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_DUPFIXED);
		
		final String resourceName = "panako_resource_map";        
		resourceMap = env.openDbi(resourceName, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
		
		storeQueue = new HashMap<Long, List<long[]>>();
		deleteQueue = new HashMap<Long, List<long[]>>();
		queryQueue = new HashMap<Long, List<Long>>();
	}

	/**
