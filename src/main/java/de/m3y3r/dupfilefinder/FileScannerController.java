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
	private File sortFile;
	private int maxObjects = 100_000;
	private static Logger log = Logger.getLogger(FileScannerController.class.getName());

	public static Comparator<SizePath> comparator = Comparator.comparingLong(SizePath::getFileSize);

	public FileScannerController(String sortFileName, File startDir) {
		this.startDir = startDir;
		this.sortFile = new File(sortFileName);
	}

	public void run() {

		log.info("Starting file scan");
		String indexName = "fileSize";

		SortingIndexWriter<SizePath> indexWriter;
		try {
			IndexWriterFactory<SizePath> indexWriterFactory = i -> {
				try {
					return new JavaSerialIndexWriter<SizePath>(new File(indexName + '.' + i));
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
		FileScannerJob.waitForJobs();

		try {
			indexWriter.close();
		} catch(IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		log.info("Sorting index. Please wait...");
		IndexReader<SizePath>[] indexReaders = null;
		IndexWriter<SizePath> indexWrt = null;
		IndexMerger<SizePath> externalSorter = new IndexMerger<SizePath>(maxObjects, comparator, indexReaders, indexWrt);
		externalSorter.run();
	}
}
