/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import jobcontrol.Job;
import jobcontrol.JobCommon;

public class FileScannerJob {

	private String sortFileName;
	private int chunkSize;
	private Logger log;
	private List<FileEntry> fileEntryList = new Vector<FileEntry>();
	private int chunkCount = 0;

	public FileScannerJob () {
	}

	public FileScannerJob (Logger log, String sortFileName, int chunkSize) {
		this.sortFileName = sortFileName;
		this.chunkSize = chunkSize;
		this.log = log;
	}

	public void scanFolder(JobCommon jc, File folder) throws InterruptedException {

		File[] files = folder.listFiles();

		if (files != null) {
			for (File file : files) {
				if (file.isDirectory()) {
					try {
						if(file.getCanonicalPath().equals(file.getAbsolutePath())) {
							// use our job as template and submit us again
							Job j = new Job(jc);
							j.setMainMethodParameters(new Object[] {jc, file});
							Run.jc.submitJob(j);
						} else
							Run.log.log(Level.FINE, "Skipping {0} - Symbolic link detected!", file);
					} catch (Exception e) {
						log.log(Level.SEVERE, "Exception", e);
					}
				} else {
					FileEntry entry = new FileEntry(file.length(), file.getAbsolutePath());
					fileEntryList.add(entry);						
				}
			}
		}

		// get lock, to write out the list
		synchronized (fileEntryList) {
			if(fileEntryList.size() > chunkSize) {
				Collections.sort(fileEntryList);

				Run.log.log(Level.FINE, "Treashhold reached, writing to disk");
				File file = new File(sortFileName + "." + Thread.currentThread().getName() + "." + chunkCount);
				try {
					FileOutputStream fos =  new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(fos);
					for(FileEntry e: fileEntryList) {
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
	
	public void finish() {
		
		// get lock, to write out the list
		synchronized (fileEntryList) {
			if(fileEntryList.size() > 0) {
				Collections.sort(fileEntryList);

				Run.log.log(Level.FINE, "Treashhold reached, writing to disk");
				File file = new File(sortFileName + "." + Thread.currentThread().getName() + "." + chunkCount);
				try {
					FileOutputStream fos =  new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(fos);
					for(FileEntry e: fileEntryList) {
						oos.writeObject(e);
					}
					oos.close();
					fos.close();
				} catch (Exception e) {
					log.log(Level.SEVERE, "Exception", e);
				}
				chunkCount++;
				fileEntryList.clear();
			}
		}
	}
}
