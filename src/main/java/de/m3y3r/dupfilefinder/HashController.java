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

import common.io.index.AbstractIndexMerger;
import common.io.index.avro.AvroIndexMerger;
import common.io.index.avro.AvroSortingIndexWriter;
import de.m3y3r.dupfilefinder.avro.HashStringAvro;
import de.m3y3r.dupfilefinder.avro.SizePathAvro;

class HashController implements Runnable {

	private final int noMaxObjects = 100000;
	private final Iterator<List<SizePathAvro>> fileSizeIterator;
	private final String algorithm;
	private final int bufferSize;
	private final File sortFile;
	private final Logger log;

	static Comparator<HashStringAvro> comparator = new Comparator<HashStringAvro>() {

		public int compare(HashStringAvro o1, HashStringAvro o2) {
			return o1.getHashString().toString().compareTo(o2.getHashString().toString());
		}
	};

	public HashController(Logger log, String sortFileName, String algorithm, int bufferSize, Iterator<List<SizePathAvro>> fileSizeIterator) {

		this.log = log;
		this.sortFile = new File(sortFileName);
		this.bufferSize = bufferSize;
		this.algorithm = algorithm;
		this.fileSizeIterator = fileSizeIterator;
	}

	public void run() {

		log.info("Starting hashing of files");

		String indexName = "hash";
		AvroSortingIndexWriter<HashStringAvro> writer;
		try {
			writer = new AvroSortingIndexWriter<HashStringAvro>(HashStringAvro.getClassSchema(), sortFile, indexName, noMaxObjects, comparator);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		//FIXME:
		HashJob.algo = algorithm;
		HashJob.bufferSize = bufferSize;
		HashJob.writer = writer;
		HashJob.log = log;

		int maxTasks = ((ThreadPoolExecutor)DupFileFinder.threadpool).getMaximumPoolSize();
		try {
			while(fileSizeIterator.hasNext()) {
				// size, list of filenames
				List<SizePathAvro> entries = fileSizeIterator.next();
				// only process entry with multiple files of the same size!
				if (entries.size() > 1) {
					for(SizePathAvro fileEntry: entries) {
						HashJob j = new HashJob(fileEntry);
						DupFileFinder.threadpool.execute(j);
						HashJob.waitForJobs(maxTasks + 1);
					}
				}
			}
			// wait for jobs to finish
			HashJob.waitForJobs(0);
			writer.close();
		} catch (InterruptedException e) {
			log.log(Level.SEVERE, "Interrupted!", e);
			return;
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		log.info("Sort hash index on disk");
		try {
			AbstractIndexMerger<HashStringAvro> externalSorter = new AvroIndexMerger<HashStringAvro>(HashStringAvro.class, sortFile, indexName, noMaxObjects, comparator, true);
			externalSorter.run();
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}
	}

}
