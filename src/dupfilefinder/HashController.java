/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import jobcontrol.Job;
import jobcontrol.JobCommon;

class HashController {

	private String algorithm;
	private int bufferSize;
	private String sortFileName;
	private final int noMaxObjects = 500000;

	// key = hash as hex string, value = file paths
	private Map<String, List<String>> duplicates = new HashMap<String, List<String>>();

	public HashController(String sortFileName, String algorithm, int bufferSize) {

		// set static values of runner class
		this.sortFileName=sortFileName;
		this.bufferSize=bufferSize;
		this.algorithm=algorithm;
//		this.chunkSize=chunkSize;
//		this.log=log;
	}

	Iterator<Entry<String, List<String>>> findDuplicates(Iterator<Entry<Long, List<String>>> fileSizeIterator) throws NoSuchAlgorithmException, InterruptedException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, FileNotFoundException, IOException, ClassNotFoundException {

		Run.log.info("Starting hashing of files");

		duplicates.clear();
//		public HashJob(int bufferSize, String algorithm, String sortFileName, int chunkSize, Logger log) throws NoSuchAlgorithmException {

		// create common job data -> call HashJob.run(FileEntry e);
		JobCommon jc = new JobCommon(HashJob.class,
				new Class[]  {int.class, String.class, String.class, int.class, Logger.class},
				new Object[] {this.bufferSize, this.algorithm, this.sortFileName, this.noMaxObjects, Run.log},
				"doHash",
				new Class[] {FileEntry.class}, true);

		while(fileSizeIterator.hasNext()) {
			//    size, list of filenames
			Entry<Long, List<String>> entry = fileSizeIterator.next();
			// only process entry with multiple files of the same size!
			if (entry.getValue().size() > 1) {
				List<String> filePaths = entry.getValue();
				Iterator<String> i1 = filePaths.iterator();
				while(i1.hasNext()) {

					FileEntry fe = new FileEntry(entry.getKey(), i1.next());
					Job j = new Job(jc);
					j.setMainMethodParameters(new Object[] {fe});
					Run.jc.submitJob(j);
				}
			}
		}

		// wait for jobs to finish
		Run.jc.waitForEmptyQueue();

		// write buffers to disk and destroy job class in worker threads
		Run.jc.finishClass(HashJob.class);
		
		Run.log.info("Sort file list on disk");
		SerializedMerger<HashEntry> externalSorter = new SerializedMerger<HashEntry>(sortFileName, noMaxObjects);
		externalSorter.mergeSortedFiles();
		externalSorter.close(true);
	
		return new SortedListToMapEntryIterator<String, String,HashEntry>(sortFileName);
	}

}
