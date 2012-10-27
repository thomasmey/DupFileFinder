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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

class HashController implements Callable<Iterator<Map.Entry<String, List<Map.Entry<String, Long>>>>> {

	private final int noMaxObjects = 500000;
	private final Iterator<Map.Entry<Long, List<String>>> fileSizeIterator;
	private final String algorithm;
	private final int bufferSize;
	private final String sortFileName;

	// key = hash as hex string, value = file paths
	private Map<String, List<String>> duplicates = new HashMap<String, List<String>>();

	public HashController(String sortFileName, String algorithm, int bufferSize, Iterator<Map.Entry<Long, List<String>>> fileSizeIterator) {

		this.sortFileName=sortFileName;
		this.bufferSize=bufferSize;
		this.algorithm=algorithm;
		this.fileSizeIterator = fileSizeIterator;
	}

	public Iterator<Map.Entry<String, List<Map.Entry<String, Long>>>> call() throws NoSuchAlgorithmException, InterruptedException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, FileNotFoundException, IOException, ClassNotFoundException {

		DupFileFinder.log.info("Starting hashing of files");

		duplicates.clear();
//		public HashJob(int bufferSize, String algorithm, String sortFileName, int chunkSize, Logger log) throws NoSuchAlgorithmException {

		while(fileSizeIterator.hasNext()) {
			//    size, list of filenames
			Map.Entry<Long, List<String>> entry = fileSizeIterator.next();
			// only process entry with multiple files of the same size!
			if (entry.getValue().size() > 1) {
				List<String> filePaths = entry.getValue();
				Iterator<String> i1 = filePaths.iterator();
				while(i1.hasNext()) {

					Map.Entry<Long, String> fileEntry = new AbstractMap.SimpleEntry<Long,String>(entry.getKey(), i1.next());
					HashJob j = new HashJob(this.bufferSize, this.algorithm, this.sortFileName, this.noMaxObjects, DupFileFinder.log, fileEntry);
					DupFileFinder.threadpool.submit(j);
				}
			}
		}

		// wait for jobs to finish
		FileScannerJob.waitForJobs();

		Comparator<Map.Entry<String, Map.Entry<String, Long>>> comparator = new Comparator<Map.Entry<String, Map.Entry<String, Long>>>() {

			@Override
			public int compare(Entry<String, Entry<String, Long>> o1,
					Entry<String, Entry<String, Long>> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		};

		DupFileFinder.log.info("Sort file list on disk");
		File dir = new File(System.getProperty("user.dir"));
		SerializedMerger<Map.Entry<String, Map.Entry<String, Long>>> externalSorter = new SerializedMerger<Map.Entry<String, Map.Entry<String, Long>>>(dir, sortFileName, noMaxObjects, comparator);
		externalSorter.mergeSortedFiles();
		externalSorter.close(true);
	
		return new SortedListToMapEntryIterator<String, Map.Entry<String, Long>, Map.Entry<String, Map.Entry<String, Long>>>(sortFileName);
	}

}
