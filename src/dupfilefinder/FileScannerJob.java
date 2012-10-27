/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileScannerJob implements Runnable {

	private final int chunkSize;
	private final File folder;

	private static int chunkCount;
	private static String sortFileName;
	private static Logger log;
	private static Integer jobCount = new Integer(0);

	private static final List<Map.Entry<Long, String>> fileEntryList = new Vector<Map.Entry<Long, String>>();
	private static final Comparator<Map.Entry<Long, String>> comparator = new Comparator<Map.Entry<Long, String>>() {

		@Override
		public int compare(Map.Entry<Long, String> o1,
				Map.Entry<Long, String> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}
	};

	public FileScannerJob (Logger log, String sortFileName, int chunkSize, File folder) {
		FileScannerJob.sortFileName = sortFileName;
		FileScannerJob.log = log;

		this.chunkSize = chunkSize;
		this.folder = folder;

		synchronized (jobCount) {
			jobCount++;
		}
	}

	public void run() {

		File[] files = folder.listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isDirectory()) {
					try {
						if(file.getCanonicalPath().equals(file.getAbsolutePath())) {
							FileScannerJob j = new FileScannerJob(FileScannerJob.log, FileScannerJob.sortFileName, this.chunkSize, file);
							DupFileFinder.threadpool.submit(j);
						} else
							DupFileFinder.log.log(Level.FINE, "Skipping {0} - Symbolic link detected!", file);
					} catch (Exception e) {
						log.log(Level.SEVERE, "Exception", e);
					}
				} else {
					Map.Entry<Long, String> entry = new AbstractMap.SimpleEntry<Long, String>(file.length(), file.getAbsolutePath());
					fileEntryList.add(entry);
				}
			}
		}

		writeList(chunkSize);
		synchronized (jobCount) {
			jobCount--;
			notifyAll();
		}
	}

	private static void writeList(int maxSize) {

		// get lock, to write out the list
		synchronized (fileEntryList) {
			if(fileEntryList.size() > maxSize) {
				Collections.sort(fileEntryList, comparator);

				DupFileFinder.log.log(Level.FINE, "Treashhold reached, writing to disk");
				File file = new File(sortFileName + "." + Thread.currentThread().getName() + "." + chunkCount);
				try {
					FileOutputStream fos =  new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(fos);
					for(Map.Entry<Long, String> e: fileEntryList) {
						oos.writeObject(e);
					}
					oos.close();
				} catch (Exception e) {
					log.log(Level.SEVERE, "Exception", e);
				}
				chunkCount++;
				fileEntryList.clear();
			}
		}
	}

	public static void waitForJobs() {
		synchronized (jobCount) {
			while(jobCount > 0)
				try {
					jobCount.wait();
				} catch (InterruptedException e) {}
		}

		writeList(0);
	}
}
