/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

import common.io.objectstream.index.IndexMerger;
import common.io.objectstream.index.IndexWriter;


class HashController implements Callable<Iterator<Map.Entry<String, List<Map.Entry<String, Long>>>>> {

	private final int noMaxObjects = 500000;
	private final Iterator<Map.Entry<Long, List<String>>> fileSizeIterator;
	private final String algorithm;
	private final int bufferSize;
	private final File sortFile;

	public HashController(String sortFileName, String algorithm, int bufferSize, Iterator<Map.Entry<Long, List<String>>> fileSizeIterator) {

		this.sortFile = new File(sortFileName);
		this.bufferSize = bufferSize;
		this.algorithm = algorithm;
		this.fileSizeIterator = fileSizeIterator;
	}

	public Iterator<Map.Entry<String, List<Map.Entry<String, Long>>>> call() throws NoSuchAlgorithmException, InterruptedException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, FileNotFoundException, IOException, ClassNotFoundException {

		DupFileFinder.log.info("Starting hashing of files");

		Comparator<Map.Entry<String, Map.Entry<String, Long>>> comparator = new Comparator<Map.Entry<String, Map.Entry<String, Long>>>() {

			@Override
			public int compare(Map.Entry<String, Map.Entry<String, Long>> o1,
					Map.Entry<String, Map.Entry<String, Long>> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		};

		IndexWriter<Map.Entry<String, Map.Entry<String, Long>>> writer = new IndexWriter<Map.Entry<String, Map.Entry<String, Long>>>(this.sortFile,"hash",comparator);

		HashJob.algo = algorithm;
		HashJob.bufferSize = bufferSize;
		HashJob.writer = writer;
		HashJob.log = DupFileFinder.log;

		int maxTasks = ((ThreadPoolExecutor)DupFileFinder.threadpool).getMaximumPoolSize();
		while(fileSizeIterator.hasNext()) {
			// size, list of filenames
			Map.Entry<Long, List<String>> entry = fileSizeIterator.next();
			// only process entry with multiple files of the same size!
			if (entry.getValue().size() > 1) {
				List<String> filePaths = entry.getValue();
				Iterator<String> i1 = filePaths.iterator();
				while(i1.hasNext()) {

					Map.Entry<Long, String> fileEntry = new AbstractMap.SimpleEntry<Long,String>(entry.getKey(), i1.next());
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
		IndexMerger<Map.Entry<String, Map.Entry<String, Long>>> externalSorter = new IndexMerger<Map.Entry<String, Map.Entry<String, Long>>>(sortFile, "hash", noMaxObjects, comparator, true);
		externalSorter.call();
		File index = externalSorter.getIndexFile();

		return new IndexEntryIterator<String, Map.Entry<String, Long>, Map.Entry<String, Map.Entry<String, Long>>>(index);
	}

}
