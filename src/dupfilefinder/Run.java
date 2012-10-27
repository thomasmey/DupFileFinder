/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import jobcontrol.JobController;

class Run {

	static Logger log;
	static JobController jc;

	public static void main(String[] args) throws Exception {

		log = Logger.getLogger("mainLog");
		log.info("Starting.");

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		if(args.length == 0) {
			log.severe("\"arg[0] = Filename\" is missing!");
			return;
		}
		File startDir = new File(args[0]);

		jc = new JobController(log, Runtime.getRuntime().availableProcessors() + 1);

		log.info("Build file list.");

		FileScannerController fsc = new FileScannerController("fileSizeArray.bin", startDir);
		Iterator<Entry<Long, List<String>>> fileSizeIterator = fsc.doScan();

		if(args.length == 2 && args[1].equals("onlyFileList")) {
			log.info("Only printing file list");
			while(fileSizeIterator.hasNext()) {
				Entry<Long, List<String>> ent = fileSizeIterator.next();
				if(ent.getValue().size() > 1) {
					System.out.print("Files with size: ");
					System.out.println(ent.getKey());
					for(String filename: ent.getValue()) {
						System.out.print("\t");
						System.out.println(filename);
					}					
				}
			}
			jc.stopWorkers();
			log.info("Finished.");
			return;
		}

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		log.info("Looking for duplicates");
		HashController checker = new HashController("hashListArray.bin", "SHA-1", 6 * 1024 * 1024);

		Iterator<Entry<String, List<String>>> dupIter = checker.findDuplicates(fileSizeIterator);

		log.info("Print duplicate files");
		while(dupIter.hasNext()) {
			Entry<String, List<String>> entry = dupIter.next();
			if(entry.getValue().size() > 1) {
				System.out.print("Duplicate files found for hash: ");
				System.out.println(entry.getKey());
				for(String filename: entry.getValue()) {
					System.out.print("\t");
					System.out.println(filename);
				}
			}
		}

		jc.stopWorkers();

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		log.info("Finished.");
	}

}
