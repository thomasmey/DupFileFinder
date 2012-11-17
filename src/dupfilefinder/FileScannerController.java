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
import java.util.concurrent.ThreadPoolExecutor;

import common.io.objectstream.index.IndexMerger;
import common.io.objectstream.index.IndexWriter;


class FileScannerController implements Callable<Iterator<Map.Entry<Long, List<String>>>>{

	private File startDir;
	private File sortFile;
	private final int noMaxObjects = 50000;

	public FileScannerController(String sortFileName, File startDir) throws Exception {
		this.startDir = startDir;
		this.sortFile = new File(sortFileName);
	}

	public Iterator<Map.Entry<Long, List<String>>> call() throws InterruptedException, FileNotFoundException, IOException, ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		DupFileFinder.log.info("Starting file scan");

		Comparator<Map.Entry<Long, String>> comparator = new Comparator<Map.Entry<Long, String>>() {

			@Override
			public int compare(Map.Entry<Long, String> o1,
					Map.Entry<Long, String> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		};

		IndexWriter<Map.Entry<Long, String>> writer = new IndexWriter<Map.Entry<Long, String>>(this.sortFile,"filesize",comparator);

		FileScannerJob j = new FileScannerJob(DupFileFinder.log, startDir, writer);
		j.run();

		// wait for jobs to finish
		FileScannerJob.waitForJobs();
		writer.close();
		long ctc = ((ThreadPoolExecutor)DupFileFinder.threadpool).getCompletedTaskCount();
		System.err.println("ctc= " + ctc);

		DupFileFinder.log.info("Sorting index. Please wait...");
		IndexMerger<Map.Entry<Long, String>> externalSorter = new IndexMerger<Map.Entry<Long, String>>(sortFile, "filesize", noMaxObjects, comparator, true);
		externalSorter.call();
		File index = externalSorter.getIndexFile();

		return new IndexEntryIterator<Long,String,Map.Entry<Long, String>>(index);
	}

}
