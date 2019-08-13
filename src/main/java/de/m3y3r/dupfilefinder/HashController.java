/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.index.IndexReader;
import common.io.index.IndexWriter;
import common.io.index.IndexWriterFactory;
import common.io.index.impl.IndexMerger;
import common.io.index.impl.SortingIndexWriter;
import de.m3y3r.dupfilefinder.model.HashString;
import de.m3y3r.dupfilefinder.model.SizePath;

class HashController implements Runnable {

	private static final Logger log = Logger.getLogger(HashController.class.getName());

	private final int noMaxObjects = 100000;
	private final Iterator<List<SizePath>> fileSizeIterator;

	private static int jobCount;

	static Comparator<HashString> comparator = new Comparator<HashString>() {
		public int compare(HashString o1, HashString o2) {
			return o1.getHashString().toString().compareTo(o2.getHashString().toString());
		}
	};

	public HashController(String algorithm, Iterator<List<SizePath>> fileSizeIterator) {
		this.fileSizeIterator = fileSizeIterator;
	}

	public void run() {

		log.info("Starting hashing of files");

		String indexName = "hash";
		SortingIndexWriter<HashString> writer;
		try {
			IndexWriterFactory<HashString> indexWriterFactory = i -> {
				try {
					return new JavaSerialIndexWriter<>(new File(indexName + '.' + i + ".part"));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			};
			writer = new SortingIndexWriter<HashString>(indexWriterFactory, noMaxObjects, comparator);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		int maxTasks = ((ThreadPoolExecutor)DupFileFinder.threadpool).getMaximumPoolSize();
		try {
			while(fileSizeIterator.hasNext()) {
				// size, list of filenames
				List<SizePath> entries = fileSizeIterator.next();
				// only process entry with multiple files of the same size!
				if (entries.size() > 1) {
					log.log(Level.INFO, "Processing {0} files each of size {1}", new Object[] {entries.size(), entries.get(0).getFileSize()});
					for(SizePath fileEntry: entries) {
						HashJob j = new HashJob(fileEntry, writer);
						DupFileFinder.threadpool.execute(j);
						waitForJobs(maxTasks + 1);
					}
				}
			}
			// wait for jobs to finish
			waitForJobs(0);
			writer.close();
		} catch (InterruptedException e) {
			log.log(Level.SEVERE, "Interrupted!", e);
			return;
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		log.info("Sort hash index on disk");
		File[] indexParts = new File(".").listFiles(f -> f.getName().endsWith(".part"));
		IndexReader<HashString>[] indexReaders = new IndexReader[indexParts.length];
		for(int i = 0, n = indexParts.length; i < n; i++) {
			try {
				indexReaders[i] = new JavaSerialIndexReader<HashString>(indexParts[i]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		IndexWriter<HashString> indexWriter = null;
		try {
			indexWriter = new JavaSerialIndexWriter<>(new File(indexName + ".index"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IndexMerger<HashString> externalSorter = new IndexMerger<HashString>(noMaxObjects, comparator, indexReaders, indexWriter);
		externalSorter.run();
	}

	public static void waitForJobs(int count) throws InterruptedException {
		synchronized (HashJob.class) {
			while(jobCount > count) {
				HashJob.class.wait();
			}
		}
	}

	public static void decJobCount() {
		synchronized (HashJob.class) {
			jobCount -= 1;
			HashJob.class.notifyAll();
		}
	}

	public static void incJobCount() {
		synchronized (HashJob.class) {
			jobCount += 1;
		}
	}

}
