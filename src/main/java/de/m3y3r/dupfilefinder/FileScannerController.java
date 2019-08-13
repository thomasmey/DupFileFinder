/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.index.IndexReader;
import common.io.index.IndexWriter;
import common.io.index.IndexWriterFactory;
import common.io.index.impl.IndexMerger;
import common.io.index.impl.SortingIndexWriter;
import de.m3y3r.dupfilefinder.model.SizePath;

class FileScannerController implements Runnable {

	private File startDir;
	private int maxObjects = 100_000;
	private static Logger log = Logger.getLogger(FileScannerController.class.getName());

	public static Comparator<SizePath> comparator = Comparator.comparingLong(SizePath::getFileSize);

	public FileScannerController(File startDir) {
		this.startDir = startDir;
	}

	public void run() {

		log.info("Starting file scan");
		String indexName = "fileSize";

		SortingIndexWriter<SizePath> indexWriter;
		try {
			IndexWriterFactory<SizePath> indexWriterFactory = i -> {
				try {
					return new JavaSerialIndexWriter<SizePath>(new File(indexName + '.' + i + ".part"));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			};
			indexWriter = new SortingIndexWriter<SizePath>(indexWriterFactory, maxObjects, comparator);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		FileScannerJob j = new FileScannerJob(startDir, indexWriter);
		j.run();

		// wait for jobs to finish
		waitForJobs();

		try {
			indexWriter.close();
		} catch(IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		log.info("Sorting index. Please wait...");
		File[] indexParts = new File(".").listFiles(f -> f.getName().endsWith(".part"));
		IndexReader<SizePath>[] indexReaders = new IndexReader[indexParts.length];
		for(int i = 0, n = indexParts.length; i < n; i++) {
			try {
				indexReaders[i] = new JavaSerialIndexReader<SizePath>(indexParts[i]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		IndexWriter<SizePath> indexWrt = null;
		try {
			indexWrt = new JavaSerialIndexWriter<>(new File(indexName + ".index"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IndexMerger<SizePath> externalSorter = new IndexMerger<SizePath>(maxObjects, comparator, indexReaders, indexWrt);
		externalSorter.run();
	}

	private static int jobCount;

	public static void incJobCount() {
		synchronized (FileScannerController.class) {
			jobCount++;
		}
	}

	public static void waitForJobs() {
		synchronized (FileScannerController.class) {
			while(jobCount > 0)
				try {
					FileScannerController.class.wait();
				} catch (InterruptedException e) {}
		}
	}

	public static void decJobCount() {
		synchronized (FileScannerController.class) {
			jobCount--;
			FileScannerController.class.notifyAll();
		}
	}

}
