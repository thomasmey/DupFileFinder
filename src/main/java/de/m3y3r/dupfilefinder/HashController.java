/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

import common.io.index.AbstractIndexMerger;
import common.io.index.avro.AvroIndexMerger;
import common.io.index.avro.AvroSortingIndexWriter;
import de.m3y3r.dupfilefinder.avro.HashStringAvro;
import de.m3y3r.dupfilefinder.avro.SizePathAvro;

class HashController implements Callable<Iterator<List<HashStringAvro>>> {

	private final int noMaxObjects = 100000;
	private final Iterator<List<SizePathAvro>> fileSizeIterator;
	private final String algorithm;
	private final int bufferSize;
	private final File sortFile;
	private final Logger log;

	private static Comparator<HashStringAvro> comparator = new Comparator<HashStringAvro>() {

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

	public Iterator<List<HashStringAvro>> call() throws NoSuchAlgorithmException, InterruptedException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, FileNotFoundException, IOException, ClassNotFoundException {

		log.info("Starting hashing of files");

		String indexName = "hash";
		AvroSortingIndexWriter<HashStringAvro> writer = new AvroSortingIndexWriter<HashStringAvro>(HashStringAvro.getClassSchema(), sortFile, indexName, noMaxObjects, comparator);

		//FIXME:
		HashJob.algo = algorithm;
		HashJob.bufferSize = bufferSize;
		HashJob.writer = writer;
		HashJob.log = DupFileFinder.log;

		int maxTasks = ((ThreadPoolExecutor)DupFileFinder.threadpool).getMaximumPoolSize();
		maxTasks = 4;
		while(fileSizeIterator.hasNext()) {
			// size, list of filenames
			List<SizePathAvro> entries = fileSizeIterator.next();
			// only process entry with multiple files of the same size!
			if (entries.size() > 1) {
				for(SizePathAvro fileEntry: entries) {
					HashJob j = new HashJob(fileEntry);
					DupFileFinder.threadpool.execute(j);
					HashJob.waitForJobs(maxTasks);
				}
			}
		}

		// wait for jobs to finish
		HashJob.waitForJobs(0);
		writer.close();

		DupFileFinder.log.info("Sort file list on disk");
		AbstractIndexMerger<HashStringAvro> externalSorter = new AvroIndexMerger<HashStringAvro>(HashStringAvro.class, sortFile, indexName, noMaxObjects, comparator, true);
		externalSorter.run();
		File index = externalSorter.getIndexFile();

		return new IndexEntryIterator<HashStringAvro>(index, HashStringAvro.class, comparator);
	}

}
