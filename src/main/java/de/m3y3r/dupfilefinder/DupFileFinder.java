/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.index.IndexReader;
import de.m3y3r.dupfilefinder.model.HashString;
import de.m3y3r.dupfilefinder.model.SizePath;

class DupFileFinder implements Runnable {

	private static Logger log = Logger.getLogger(DupFileFinder.class.getName());
	static ExecutorService threadpool;
	private String args[];

	public DupFileFinder(String[] args) {
		this.args = args;
	}

	public static void main(String[] args) {
		new DupFileFinder(args).run();
	}

	public void run() {

		log.info("Starting.");

		log.log(Level.FINE, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		if(args.length == 0) {
			log.severe("\"arg[0] = Command is missing! - Aborting");
			return;
		}

		log.log(Level.INFO, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});

		int nThreads = Runtime.getRuntime().availableProcessors();
		threadpool = Executors.newFixedThreadPool(nThreads);

		String cmd = args[0];
		try {
			processCommand(cmd);
		} catch (ClassNotFoundException e) {
			log.log(Level.SEVERE, "Error!", e);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
		}

		threadpool.shutdown();

		log.log(Level.INFO, "Memory usage: {0} total bytes, {1} free bytes", new Object[] {Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()});
		log.info("Finished.");
	}

	private void processCommand(String cmd) throws ClassNotFoundException, IOException {

		if("searchDuplicates".equals(cmd)) {
			processCommand("buildfileSizeIndex");
			processCommand("buildHashIndex");
			processCommand("printHashIndex");
		} else if("buildfileSizeIndex".equals(cmd)) {
			File startDir = new File(args[1]);
			buildFileSizeIndex(startDir);
		} else if("buildHashIndex".equals(cmd)) {
			buildHashIndex();
		} else if("printHashIndex".equals(cmd)) {
			printHashIndex();
		} else if("printFileSizeIndex".equals(cmd)) {
			printFileSizeIndex();
		} else if("dedpulicateFiles(String, String)".equals(cmd)) {
			dedpulicateFiles(args[1]);
		} else {
			log.log(Level.SEVERE,"\"{0}\" = Unknown command! - Aborting", cmd);
			return;
		}
	}

	private void buildHashIndex() throws ClassNotFoundException, IOException {

		log.info("Build hash index.");
		File index = new File("fileSize.index");
		IndexReader<SizePath> indexReader = new JavaSerialIndexReader<SizePath>(index);
		Iterator<List<SizePath>> fileSizeIterator = new IndexEntryIterator<SizePath>(indexReader, FileScannerController.comparator);
		HashController checker = new HashController("SHA-1", fileSizeIterator);
		checker.run();
	}

	private void buildFileSizeIndex(File startDir) {
		log.info("Build file size index.");
		FileScannerController fsc = new FileScannerController(startDir);
		fsc.run();
	}

	private void printFileSizeIndex() throws ClassNotFoundException, IOException {
		log.info("Only printing file list");
		File index = new File("fileSize.index");
		IndexReader<SizePath> indexReader = new JavaSerialIndexReader<SizePath>(index);
		Iterator<List<SizePath>> fileSizeIterator = new IndexEntryIterator<SizePath>(indexReader, FileScannerController.comparator);

		PrintWriter out = new PrintWriter("filesize-list.txt");
		while(fileSizeIterator.hasNext()) {
			List<SizePath> ent = fileSizeIterator.next();
			if(ent.size() > 1) {
				out.print("Files with size: ");
				out.println(ent.get(0).getFileSize());
				for(SizePath e: ent) {
					out.print("\t");
					out.println(e.getFilePath());
				}
			}
		}
		out.close();
	}

	private void dedpulicateFiles(String fileNameSuffix) throws ClassNotFoundException, IOException {

		log.info("Deduplicate files with hardlinks");
		File index = new File("hash.index");
		IndexReader<HashString> indexReader = new JavaSerialIndexReader<HashString>(index);
		Iterator<List<HashString>> dupIter = new IndexEntryIterator<HashString>(indexReader, HashController.comparator);
		while(dupIter.hasNext()) {
			List<HashString> entries = dupIter.next();
			int n = entries.size();
			if(n <= 1)
				continue;

			// check if all file names end with the given suffix
			File firstFile = null;
			boolean allEndingsAreOkay = true;
			for(int i = 0; i < n; i++) {
				HashString entry = entries.get(i);
				String path = entry.getFilePath().toString();
				if(!path.endsWith(fileNameSuffix)) {
					allEndingsAreOkay = false;
					break;
				}
				if(i == 0) {
					firstFile = new File(path);
				}
			}

			if(!allEndingsAreOkay)
				continue;

			assert firstFile != null;
			Path existing = firstFile.toPath();
			for(int i = 1; i < n; i++) {
				HashString entry = entries.get(i);
				String path = entry.getFilePath().toString();
				File link = new File(path);
				link.delete();
				Files.createLink(link.toPath(), existing);
			}
		}
	}

	private void printHashIndex() throws ClassNotFoundException, IOException {

		log.info("Print duplicate files");
		long dupBytes = 0;
		File index = new File("hash.index");
		IndexReader<HashString> indexReader = new JavaSerialIndexReader<HashString>(index);
		Iterator<List<HashString>> dupIter = new IndexEntryIterator<HashString>(indexReader, HashController.comparator);

		PrintWriter out = new PrintWriter("duplicates-hash.txt");
		while(dupIter.hasNext()) {
			List<HashString> entries = dupIter.next();
			if(entries.size() > 1) {
				dupBytes = dupBytes + (entries.size() - 1) * entries.get(0).getFileSize();
				out.print("Duplicate files found for hash: ");
				out.print(entries.get(0).getHashString());
				out.println(" - size: " + entries.get(0).getFileSize());
				for(HashString entry: entries) {
					out.print("\t");
					out.println(entry.getFilePath());
				}
			}
		}
		out.println("Duplicates in bytes: " + dupBytes);
		out.close();
	}
}
