package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.Executors;

import org.junit.Test;

import common.io.index.IndexWriterFactory;
import common.io.index.impl.SortingIndexWriter;
import de.m3y3r.dupfilefinder.model.SizePath;

public class FileScannerJobTest {

	@Test
	public void test() throws IOException {
		int nThreads = Runtime.getRuntime().availableProcessors();
		DupFileFinder.threadpool = Executors.newFixedThreadPool(nThreads);

		File folderToScan = new File("C:\\Users\\thomas\\");
		IndexWriterFactory<SizePath> indexWriterFactory = i -> {
			try {
				return new JavaSerialIndexWriter(new File("test.index." + i));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		};
		SortingIndexWriter<SizePath> indexWriter = new SortingIndexWriter<SizePath>(indexWriterFactory, 100_000, Comparator.comparingLong(SizePath::getFileSize));
		FileScannerJob job = new FileScannerJob(folderToScan, indexWriter);
		job.run();
		FileScannerController.waitForJobs();
	}
}
