/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;

class FileScannerController implements Callable<Iterator<Map.Entry<Long, List<String>>>>{

	private File startDir;
	private String sortFileName;
	private final int noMaxObjects = 500000;

	public FileScannerController(String sortFileName, File startDir) throws Exception {
		this.startDir = startDir;
		this.sortFileName = sortFileName;
	}

	public Iterator<Map.Entry<Long, List<String>>> call() throws InterruptedException, FileNotFoundException, IOException, ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		DupFileFinder.log.info("Starting file scan");

		int dirCount=0;
		File[] files = startDir.listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isDirectory()) {
					try {
						if(file.getCanonicalPath().equals(file.getAbsolutePath())) {
							// submit folder as job
							FileScannerJob j = new FileScannerJob(DupFileFinder.log, this.sortFileName, this.noMaxObjects, file);
							DupFileFinder.threadpool.submit(j);
							dirCount++;
						} else
							DupFileFinder.log.log(Level.FINE, "Skipping {0} - Symbolic link detected!", file);
					} catch (IOException e) {
						DupFileFinder.log.log(Level.SEVERE, "IOException", e);
					}
				}
			}
		}

		// startDir didn't contain any folders, submit startDir as job
		if(dirCount == 0) {
			FileScannerJob j = new FileScannerJob(DupFileFinder.log, this.sortFileName, this.noMaxObjects, startDir);
			DupFileFinder.threadpool.submit(j);
		}

		// wait for jobs to finish
		FileScannerJob.waitForJobs();

		Comparator<Map.Entry<Long, String>> comparator = new Comparator<Map.Entry<Long, String>>() {

			@Override
			public int compare(Map.Entry<Long, String> o1,
					Map.Entry<Long, String> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		};

		DupFileFinder.log.info("Sort file list on disk");
		File dir = new File(System.getProperty("user.dir"));
		SerializedMerger<Map.Entry<Long, String>> externalSorter = new SerializedMerger<Map.Entry<Long, String>>(dir, sortFileName, noMaxObjects, comparator);
		externalSorter.mergeSortedFiles();
		externalSorter.close(true);
	
		return new SortedListToMapEntryIterator<Long,String,Map.Entry<Long, String>>(sortFileName);
	}

}
