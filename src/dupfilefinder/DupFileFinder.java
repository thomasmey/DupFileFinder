/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

class DupFileFinder {

	static Logger log;
	static ExecutorService threadpool;

	public static void main(String[] args) throws Exception {

		log = Logger.getLogger("mainLog");
		log.info("Starting.");

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		if(args.length == 0) {
			log.severe("\"arg[0] = Filename\" is missing!");
			return;
		}
		File startDir = new File(args[0]);

		threadpool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);

		log.info("Build file list.");

		FileScannerController fsc = new FileScannerController("fileSizeArray.bin", startDir);
		Iterator<Map.Entry<Long, List<String>>> fileSizeIterator = fsc.call();

		if(args.length == 2 && args[1].equals("onlyFileList")) {
			log.info("Only printing file list");
			while(fileSizeIterator.hasNext()) {
				Map.Entry<Long, List<String>> ent = fileSizeIterator.next();
				if(ent.getValue().size() > 1) {
					System.out.print("Files with size: ");
					System.out.println(ent.getKey());
					for(String filename: ent.getValue()) {
						System.out.print("\t");
						System.out.println(filename);
					}					
				}
			}
			threadpool.shutdown();
			log.info("Finished.");
			return;
		}

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		log.info("Looking for duplicates");
		HashController checker = new HashController("hashListArray.bin", "SHA-1", 6 * 1024 * 1024, fileSizeIterator);

		Iterator<Map.Entry<String, List<Map.Entry<String, Long>>>> dupIter = checker.call();

		log.info("Print duplicate files");
		while(dupIter.hasNext()) {
			Map.Entry<String, List<Map.Entry<String, Long>>> entry = dupIter.next();
			if(entry.getValue().size() > 1) {
				System.out.print("Duplicate files found for hash: ");
				System.out.println(entry.getKey());
				for(Map.Entry<String, Long> filename: entry.getValue()) {
					System.out.print("\t");
					System.out.println(filename.getKey());
				}
			}
		}

		threadpool.shutdown();

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		log.info("Finished.");
	}
}
