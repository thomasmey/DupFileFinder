/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.m3y3r.dupfilefinder.model.HashString;
import de.m3y3r.dupfilefinder.model.SizePath;

public class HashJob implements Runnable {

	private final SizePath fileEntry;

	private static int jobCount;

	private final static Logger log = Logger.getLogger(HashJob.class.getName());
	private final static String algo = "SHA-1";

//	static AvroSortingIndexWriter<HashStringAvro> writer;

	private static ThreadLocal<MessageDigest> messageDigest = new ThreadLocal<MessageDigest>() {
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance(algo);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return null;
		}
	};

	public HashJob(SizePath fileEntry) {
		this.fileEntry = fileEntry;

		synchronized (HashJob.class) {
			jobCount += 1;
		}
	}

	public void run() {

		log.info("start");
		String hashString;
		if(fileEntry.getFileSize() > 0) {
			hashString = calculateHash(Paths.get(fileEntry.getFilePath().toString()));
		} else {
			hashString = "00";
		}
		log.info("end");

		if(hashString == null) {
			System.out.println("error!");
			return;
		}
		System.out.println("hash: " + hashString);

		// HashString -> (FileName, FileSize)
		HashString he = new HashString(hashString, fileEntry.getFilePath(), fileEntry.getFileSize());
//		synchronized (writer) {
//			try {
//				writer.writeObject(he);
//			} catch (IOException e) {
//				log.log(Level.SEVERE, "Error!", e);
//			}
//		}

		synchronized (HashJob.class) {
			jobCount -= 1;
			HashJob.class.notifyAll();
		}
	}

	public static void waitForJobs(int count) throws InterruptedException {
		synchronized (HashJob.class) {
			while(jobCount > count) {
				HashJob.class.wait();
			}
		}
	}

	private String calculateHash(Path filePath){
		log.log(Level.FINE, "Hashing file: {0}", filePath);
		try(FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
			MessageDigest md = messageDigest.get();
			md.reset();

			for(long position = 0, size = channel.size(); size >= 0; ) {
				long msize = Math.min(Integer.MAX_VALUE, size);
				MappedByteBuffer byteBuffer = channel.map(MapMode.READ_ONLY, position, size);
				md.update(byteBuffer);

				size -= msize;
				position += msize;
				if(size <= 0) {
					break;
				}
			}
			byte[] digest = md.digest();
			return Arrays.toString(digest);
		} catch(IOException e) {
			log.log(Level.SEVERE, "Could not read from file: {0}", filePath);
			return null;
		}
	}
}
