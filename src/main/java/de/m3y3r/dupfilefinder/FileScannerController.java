/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.Schema;

import common.io.index.avro.AvroIndexMerger;
import common.io.index.avro.AvroSortingIndexWriter;
import de.m3y3r.dupfilefinder.avro.SizePathAvro;

class FileScannerController implements Runnable {

	private File startDir;
	private File sortFile;
	private int maxObjects = 100000;
	private Logger log;

	static Comparator<SizePathAvro> comparator = new Comparator<SizePathAvro>() {

		public int compare(SizePathAvro o1, SizePathAvro o2) {
			return o2.getFileSize().compareTo(o1.getFileSize());
		}

	};

	public FileScannerController(Logger log, String sortFileName, File startDir) {
		this.startDir = startDir;
		this.sortFile = new File(sortFileName);
		this.log = log;
	}

	public void run() {

		log.info("Starting file scan");
		Schema schema = SizePathAvro.getClassSchema();
		String indexName = "fileSize";

		AvroSortingIndexWriter<SizePathAvro> indexWriter;
		try {
			indexWriter = new AvroSortingIndexWriter<SizePathAvro>(schema, sortFile, indexName, maxObjects, comparator);
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		FileScannerJob j = new FileScannerJob(log, startDir, indexWriter);
		j.run();

		// wait for jobs to finish
		FileScannerJob.waitForJobs();

		try {
			indexWriter.flush();
			indexWriter.close();
		} catch(IOException e) {
			log.log(Level.SEVERE, "Error!", e);
			return;
		}

		log.info("Sorting index. Please wait...");
		try {
			AvroIndexMerger<SizePathAvro> externalSorter = new AvroIndexMerger<SizePathAvro>(SizePathAvro.class, sortFile, indexName, maxObjects, comparator, true);
			externalSorter.run();
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error!", e);
		}
	}

}
