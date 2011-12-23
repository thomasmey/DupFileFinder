package dupfilefinder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import jobcontrol.Job;
import jobcontrol.JobCommon;

class FileScannerController {

	private File startDir;
	private String sortFileName;
	private final int noMaxObjects = 500000;

	public FileScannerController(String sortFileName, File startDir) throws Exception {
		this.startDir = startDir;
		this.sortFileName = sortFileName;
	}

	Iterator<Entry<Long, List<String>>> doScan() throws InterruptedException, FileNotFoundException, IOException, ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		Run.log.info("Starting file scan");

		// create common job data -> call FileStorageFSJob.scanFolder(JobCommon jc, File folder);
		JobCommon jc = new JobCommon(FileScannerJob.class,new Class[] {Logger.class, String.class, int.class}, new Object[] {Run.log, this.sortFileName, this.noMaxObjects}, "scanFolder", new Class[] {JobCommon.class, File.class}, true);

		int dirCount=0;
		File[] files = startDir.listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isDirectory()) {
					try {
						if(file.getCanonicalPath().equals(file.getAbsolutePath())) {
							// submit folder as job
							Job j = new Job(jc);
							j.setMainMethodParameters(new Object[] {jc, file});
							Run.jc.submitJob(j);
							dirCount++;
						} else
							Run.log.log(Level.FINE, "Skipping {0} - Symbolic link detected!", file);
					} catch (IOException e) {
						Run.log.log(Level.SEVERE, "IOException", e);
					}
				}
			}
		}

		// startDir didn't contain any folders, submit startDir as job
		if(dirCount==0) {
			Job j = new Job(jc);
			j.setMainMethodParameters(new Object[] {jc, startDir});
			Run.jc.submitJob(j);
		}

		// wait for jobs to finish
		Run.jc.waitForEmptyQueue();

		// write buffers to disk and destroy job class in worker threads
		Run.jc.finishClass(FileScannerJob.class);
//		Run.jc.removeClass(FileStorageFSJob.class);

		Run.log.info("Sort file list on disk");
		SerializedMerger<FileEntry> externalSorter = new SerializedMerger<FileEntry>(sortFileName, noMaxObjects);
		externalSorter.mergeSortedFiles();
		externalSorter.close(true);
	
		return new SortedListToMapEntryIterator<Long,String,FileEntry>(sortFileName);
	}

}
