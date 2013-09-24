/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.avro.Schema;

import common.io.index.avro.AvroIndexMerger;
import common.io.index.avro.AvroSortingIndexWriter;
import de.m3y3r.dupfilefinder.avro.SizePathAvro;

class FileScannerController implements Callable<Iterator<List<SizePathAvro>>>{

	private File startDir;
	private File sortFile;
	private int maxObjects = 100000;

	static Comparator<SizePathAvro> comparator = new Comparator<SizePathAvro>() {

		public int compare(SizePathAvro o1, SizePathAvro o2) {
			return o1.getFileSize().compareTo(o2.getFileSize());
		}

	};

	public FileScannerController(String sortFileName, File startDir) throws Exception {
		this.startDir = startDir;
		this.sortFile = new File(sortFileName);
	}

	public Iterator<List<SizePathAvro>> call() throws InterruptedException, FileNotFoundException, IOException, ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		DupFileFinder.log.info("Starting file scan");
		Schema schema = SizePathAvro.getClassSchema();
		String indexName = "fileSize";
		AvroSortingIndexWriter<SizePathAvro> indexWriter = new AvroSortingIndexWriter<SizePathAvro>(schema, sortFile, indexName, maxObjects, comparator);
		FileScannerJob j = new FileScannerJob(DupFileFinder.log, startDir, indexWriter);
		j.run();

		// wait for jobs to finish
		FileScannerJob.waitForJobs();
		indexWriter.flush();

		long ctc = ((ThreadPoolExecutor)DupFileFinder.threadpool).getCompletedTaskCount();
		System.err.println("ctc= " + ctc);

		DupFileFinder.log.info("Sorting index. Please wait...");
		AvroIndexMerger<SizePathAvro> externalSorter = new AvroIndexMerger<SizePathAvro>(SizePathAvro.class, sortFile, indexName, maxObjects, comparator, true);
		externalSorter.run();
		File index = externalSorter.getIndexFile();

		return new IndexEntryIterator<SizePathAvro>(index, SizePathAvro.class, comparator);
	}

}
