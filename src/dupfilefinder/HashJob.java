/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HashJob implements Runnable {

	private final MessageDigest messageDigest;
	private final byte[] digestBuffer;
	private final int chunkSize;
	private final Map.Entry<Long, String> fileEntry;

	private static int chunkCount;
	private static String sortFileName;
	private static Logger log;
	private static Integer jobCount = new Integer(0);

	private final static List<Map.Entry<String, Map.Entry<String, Long>>> hashEntryList = new Vector<Map.Entry<String, Map.Entry<String, Long>>>();
	private static final Comparator<Map.Entry<String, Map.Entry<String, Long>>> comparator = new Comparator<Map.Entry<String, Map.Entry<String, Long>>>() {

		@Override
		public int compare(Map.Entry<String, Map.Entry<String, Long>> o1,
				Map.Entry<String, Map.Entry<String, Long>> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}
	};

	public HashJob(int bufferSize, String algorithm, String sortFileName, int chunkSize, Logger log, Map.Entry<Long, String> fileEntry) throws NoSuchAlgorithmException {
		//throw an exception here if algorithm is not supported.
		this.messageDigest = MessageDigest.getInstance(algorithm);
		this.digestBuffer = new byte[bufferSize];

		HashJob.sortFileName = sortFileName;
		HashJob.log = log;

		this.chunkSize = chunkSize;
		this.fileEntry = fileEntry;

		synchronized (jobCount) {
			jobCount++;
		}
	}

	public void run() {

		String hashString;
		if(fileEntry.getKey()>0)
			hashString = calculateHash(fileEntry.getValue());
		else
			hashString = "00";

		Map.Entry<String, Map.Entry<String, Long>> he = new AbstractMap.SimpleEntry<String, Map.Entry<String, Long>> (hashString,
				new AbstractMap.SimpleEntry<String, Long>(fileEntry.getValue(),fileEntry.getKey()));
		hashEntryList.add(he);

		writeList(chunkSize);
		synchronized (jobCount) {
			jobCount--;
			notifyAll();
		}
	}

	private static void writeList(int maxSize) {

		// get lock, to write out the list
		synchronized (hashEntryList) {
			if(hashEntryList.size() > maxSize) {
				Collections.sort(hashEntryList, comparator);

				DupFileFinder.log.log(Level.FINE, "Treashhold reached, writing to disk");
				File file = new File(sortFileName + "." + Thread.currentThread().getName() + "." + chunkCount);
				try {
					FileOutputStream fos =  new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(fos);
					for(Map.Entry<String, Map.Entry<String, Long>> e: hashEntryList) {
						oos.writeObject(e);
					}
					oos.close();
				} catch (Exception e) {
					log.log(Level.SEVERE, "Exception", e);
				}
				chunkCount++;
				hashEntryList.clear();
			}
		}
	}

	public static void waitForJobs() {
		synchronized (jobCount) {
			while(jobCount > 0)
				try {
					jobCount.wait();
				} catch (InterruptedException e) {}
		}

		writeList(0);
	}

	private String calculateHash(String filePath){

		File file = new File(filePath);
		try {

			InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
			DigestInputStream digestInputStream = new DigestInputStream(inputStream, this.messageDigest);
		
			while(digestInputStream.read(this.digestBuffer) >= 0);
			digestInputStream.close();
			byte[] digest = digestInputStream.getMessageDigest().digest();

			return hashToHex(digest);

		} catch(Exception e) {
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
