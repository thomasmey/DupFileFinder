/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import common.io.objectstream.index.IndexWriter;

public class HashJob implements Runnable {

	private final Map.Entry<Long, String> fileEntry;

	private static AtomicInteger jobCount = new AtomicInteger(0);

	static Logger log;
	static String algo;
	static int bufferSize;
	static IndexWriter<Map.Entry<String, Map.Entry<String, Long>>> writer;

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

	public HashJob(Map.Entry<Long, String> fileEntry) throws NoSuchAlgorithmException {
		this.fileEntry = fileEntry;

		jobCount.incrementAndGet();
	}

	public void run() {

		String hashString;
		if(fileEntry.getKey() > 0)
			hashString = calculateHash(fileEntry.getValue());
		else
			hashString = "00";

		Map.Entry<String, Map.Entry<String, Long>> he = new AbstractMap.SimpleEntry<String, Map.Entry<String, Long>> (hashString,
				new AbstractMap.SimpleEntry<String, Long>(fileEntry.getValue(),fileEntry.getKey()));

		synchronized (writer) {
			writer.write(he);
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

	private String calculateHash(String filePath){

		try {
			byte[] digestBuffer = (byte[]) buffer.get();

			InputStream inputStream = new BufferedInputStream(new FileInputStream(filePath));
			MessageDigest md = messageDigest.get();
			md.reset();
			DigestInputStream digestInputStream = new DigestInputStream(inputStream, md);

			while(digestInputStream.read(digestBuffer) >= 0);
			digestInputStream.close();
			byte[] digest = digestInputStream.getMessageDigest().digest();

			return hashToHex(digest);

		} catch(IOException e) {
			DupFileFinder.log.log(Level.SEVERE, "Could not read from file: {0}", filePath);
		}
		return null;
	}

	private String hashToHex(byte[] hash) {
		StringBuilder hexString = new StringBuilder();
		String hex;
		for (byte byt : hash) {
			hex = Integer.toHexString(0xFF & byt);
			// toHexString drops leading zeros, add them again
			if (hex.length() == 1)
				hexString.append(0);
			hexString.append(hex);
		}

		return hexString.toString();
	}
}
