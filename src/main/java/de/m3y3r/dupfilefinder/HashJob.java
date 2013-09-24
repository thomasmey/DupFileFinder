/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.index.avro.AvroSortingIndexWriter;
import de.m3y3r.dupfilefinder.avro.HashStringAvro;
import de.m3y3r.dupfilefinder.avro.SizePathAvro;

public class HashJob implements Runnable {

	private final SizePathAvro fileEntry;

	private static AtomicInteger jobCount = new AtomicInteger(0);

	static Logger log;
	static String algo;
	static int bufferSize;
	static AvroSortingIndexWriter<HashStringAvro> writer;

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

	private static ThreadLocal<Object> buffer = new ThreadLocal<Object>() {
		protected Object initialValue() {
			return new byte[bufferSize];
		}
	};

	public HashJob(SizePathAvro fileEntry) throws NoSuchAlgorithmException {
		this.fileEntry = fileEntry;

		jobCount.incrementAndGet();
	}

	public void run() {

		String hashString;
		if(fileEntry.getFileSize() > 0)
			hashString = calculateHash(fileEntry.getFilePath());
		else
			hashString = "00";

		// HashString -> (FileName, FileSize)
		HashStringAvro he = new HashStringAvro(hashString, fileEntry.getFilePath(), fileEntry.getFileSize());
		synchronized (writer) {
			try {
				writer.writeObject(he);
			} catch (IOException e) {
				log.log(Level.SEVERE, "Error!", e);
			}
		}

		synchronized (jobCount) {
			jobCount.decrementAndGet();
			jobCount.notifyAll();
		}
	}

	public static void waitForJobs(int count) {
		synchronized (jobCount) {
			while(jobCount.get() > count)
				try {
					jobCount.wait();
				} catch (InterruptedException e) {}
		}
	}

	private String calculateHash(CharSequence filePath){

		log.log(Level.FINE, "Hashing file: {0}", filePath);
		DigestInputStream digestInputStream = null;

		try {
			InputStream inputStream = new BufferedInputStream(new FileInputStream(filePath.toString()));
			MessageDigest md = messageDigest.get();
			md.reset();
			digestInputStream = new DigestInputStream(inputStream, md);

			byte[] digestBuffer = (byte[]) buffer.get();

			while(digestInputStream.read(digestBuffer) >= 0);
			byte[] digest = digestInputStream.getMessageDigest().digest();

			return Arrays.toString(digest);

		} catch(IOException e) {
			log.log(Level.SEVERE, "Could not read from file: {0}", filePath);
			return "IOException";
		} finally {
			try {
				if(digestInputStream != null)
					digestInputStream.close();
			} catch(IOException e) {}
		}
	}
}
