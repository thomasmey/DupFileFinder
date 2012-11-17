/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.objectstream.index.IndexWriter;

public class FileScannerJob implements Runnable {

	private final File folder;
	private final IndexWriter<Map.Entry<Long, String>> writer;

	private static Logger log;
	private static AtomicInteger jobCount = new AtomicInteger();

	public FileScannerJob (Logger log, File folder, IndexWriter<Map.Entry<Long, String>> writer) {
		FileScannerJob.log = log;

		this.writer = writer;
		this.folder = folder;

		jobCount.incrementAndGet();
	}

	public void run() {

		File[] files = folder.listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isDirectory()) {
					try {
						if(file.getCanonicalPath().equals(file.getAbsolutePath())) {
							FileScannerJob j = new FileScannerJob(FileScannerJob.log, file, writer);
							DupFileFinder.threadpool.execute(j);
						} else
							DupFileFinder.log.log(Level.FINE, "Skipping {0} - Symbolic link detected!", file);
					} catch (IOException e) {
						log.log(Level.SEVERE, "Exception", e);
					}
				} else {
					Map.Entry<Long, String> entry = new AbstractMap.SimpleEntry<Long, String>(file.length(), file.getAbsolutePath());
					synchronized (writer) {
						writer.write(entry);
					}
				}
			}
		}

		synchronized (jobCount) {
			jobCount.decrementAndGet();
			jobCount.notifyAll();
		}
	}

	public static void waitForJobs() {
		synchronized (jobCount) {
			while(jobCount.get() > 0)
				try {
					jobCount.wait();
				} catch (InterruptedException e) {}
		}
	}
}
