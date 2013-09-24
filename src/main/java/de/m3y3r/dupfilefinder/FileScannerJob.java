/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.index.avro.AvroSortingIndexWriter;

import de.m3y3r.dupfilefinder.avro.SizePathAvro;

public class FileScannerJob implements Runnable {

	private final File folder;
	private final AvroSortingIndexWriter<SizePathAvro> indexWriter;
	private final Logger log;

	private static AtomicInteger jobCount = new AtomicInteger();

	public FileScannerJob (Logger log, File folderToScan, AvroSortingIndexWriter<SizePathAvro> indexWriter) {
		this.log = log;
		this.folder = folderToScan;
		this.indexWriter = indexWriter;
		jobCount.incrementAndGet();
	}

	public void run() {

		File[] files = folder.listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isDirectory()) {
					try {
						if(file.getCanonicalPath().equals(file.getAbsolutePath())) {
							FileScannerJob j = new FileScannerJob(log, file, indexWriter);
							DupFileFinder.threadpool.execute(j);
						} else
							DupFileFinder.log.log(Level.FINE, "Skipping {0} - Symbolic link detected!", file);
					} catch (IOException e) {
						log.log(Level.SEVERE, "Exception", e);
					}
				} else {
					SizePathAvro entry = new SizePathAvro(file.length(), file.getAbsolutePath());
					synchronized (indexWriter) {
						try {
							indexWriter.writeObject(entry);
						} catch (IOException e) {
							log.log(Level.SEVERE, "Exception", e);
						}
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
