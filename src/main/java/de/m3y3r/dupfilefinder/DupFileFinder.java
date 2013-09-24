/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.m3y3r.dupfilefinder.avro.HashStringAvro;
import de.m3y3r.dupfilefinder.avro.SizePathAvro;

class DupFileFinder {

	static Logger log;
	static ExecutorService threadpool;

	public static void main(String[] args) throws Exception {

		log = Logger.getLogger("mainLog");
		log.info("Starting.");

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		if(args.length == 0) {
			log.severe("\"arg[0] = Directory to start search\" is missing!");
			return;
		}

		int nThreads = Runtime.getRuntime().availableProcessors();
		threadpool = Executors.newFixedThreadPool(nThreads);

		Iterator<List<SizePathAvro>> fileSizeIterator;
		String sortFileName = "DupFileFinder";
		if(args.length == 2 && args[1].equals("onlyHashFiles")) {
			File index = new File(sortFileName + ".fileSize.index");
			fileSizeIterator = new IndexEntryIterator<SizePathAvro>(index, SizePathAvro.class, FileScannerController.comparator);
		} else {
			File startDir = new File(args[0]);

			log.info("Build file list.");

			FileScannerController fsc = new FileScannerController(sortFileName, startDir);
			fileSizeIterator = fsc.call();


			if(args.length == 2 && args[1].equals("onlyFileList")) {
				log.info("Only printing file list");
				PrintWriter out = new PrintWriter("filesize-list.txt");
				while(fileSizeIterator.hasNext()) {
					List<SizePathAvro> ent = fileSizeIterator.next();
					if(ent.size() > 1) {
						out.print("Files with size: ");
						out.println(ent.get(0).getFileSize());
						for(SizePathAvro e: ent) {
							out.print("\t");
							out.println(e.getFilePath());
						}
					}
				}
				out.close();
				threadpool.shutdown();
				log.info("Finished.");
				return;
			}
		}

		log.log(Level.INFO, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		log.info("Looking for duplicates");
		HashController checker = new HashController(log, sortFileName, "SHA-1", 6 * 1024 * 1024, fileSizeIterator);

		Iterator<List<HashStringAvro>> dupIter = checker.call();

		log.info("Print duplicate files");
		PrintWriter out = new PrintWriter("duplicates-hash.txt");
		while(dupIter.hasNext()) {
			List<HashStringAvro> entries = dupIter.next();
			if(entries.size() > 1) {
				out.print("Duplicate files found for hash: ");
				out.print(entries.get(0).getHashString());
				out.println(" - size: " + entries.get(0).getFileSize());
				for(HashStringAvro entry: entries) {
					out.print("\t");
					out.println(entry.getFilePath());
				}
			}
		}
		out.close();
		threadpool.shutdown();

		log.log(Level.INFO, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		log.info("Finished.");
	}
}
