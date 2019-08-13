/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.index.impl.SortingIndexWriter;
import de.m3y3r.dupfilefinder.model.SizePath;

public class FileScannerJob implements Runnable {

	private final static Logger log = Logger.getLogger(FileScannerJob.class.getName());

	private final File folder;
	private final SortingIndexWriter<SizePath> indexWriter;

	public FileScannerJob (File folderToScan, SortingIndexWriter<SizePath> indexWriter) {
		this.folder = folderToScan;
		this.indexWriter = indexWriter;
		FileScannerController.incJobCount();
	}

	public void run() {
		try {
			File[] files = folder.listFiles();

			if (files != null) {
				for (File file : files) {
					if(Files.isSymbolicLink(file.toPath())) {
						log.log(Level.FINE, "Skipping {0} - Symbolic link detected!", file);
					}

					if (file.isDirectory()) {
						FileScannerJob j = new FileScannerJob(file, indexWriter);
						DupFileFinder.threadpool.execute(j);
					} else {
						SizePath entry = new SizePath(file.length(), file.getAbsolutePath());
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
		} finally {
			FileScannerController.decJobCount();
		}
	}
}
