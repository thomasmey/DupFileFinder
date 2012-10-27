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
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HashJob {

	private MessageDigest messageDigest;
	private byte[] digestBuffer;
	private List<HashEntry> hashEntryList = new Vector<HashEntry>();
	private int chunkCount = 0;
	private String sortFileName;
	private int chunkSize;
	private Logger log;

	public HashJob() {
	}

	public HashJob(int bufferSize, String algorithm, String sortFileName, int chunkSize, Logger log) throws NoSuchAlgorithmException {
		//throw an exception here if algorithm is not supported.
		this.messageDigest = MessageDigest.getInstance(algorithm);
		this.digestBuffer = new byte[bufferSize];
		
		this.sortFileName = sortFileName;
		this.chunkSize = chunkSize;
		this.log = log;
	}

	public void doHash(FileEntry fe) {

		String hashString;
		if(fe.getKey()>0)
			hashString = calculateHash(fe.getValue());
		else
			hashString = "00";
		
		HashEntry he = new HashEntry(hashString,fe.getValue(),fe.getKey());
		hashEntryList.add(he);

		// get lock, to write out the list
		synchronized (hashEntryList) {
			if(hashEntryList.size() > chunkSize) {
				Collections.sort(hashEntryList);

				Run.log.log(Level.FINE, "Treashhold reached, writing to disk");
				File file = new File(sortFileName + "." + Thread.currentThread().getName() + "." + chunkCount);
				try {
					FileOutputStream fos =  new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(fos);
					for(HashEntry e: hashEntryList) {
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
			Run.log.log(Level.SEVERE, "Could not read from file: {0}", filePath);
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

	public void finish() {
		
		// get lock, to write out the list
		synchronized (hashEntryList) {
			if(hashEntryList.size() > 0) {
				Collections.sort(hashEntryList);

				Run.log.log(Level.FINE, "Treashhold reached, writing to disk");
				File file = new File(sortFileName + "." + Thread.currentThread().getName() + "." + chunkCount);
				try {
					FileOutputStream fos =  new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(fos);
					for(HashEntry e: hashEntryList) {
						oos.writeObject(e);
					}
					oos.close();
					fos.close();
				} catch (Exception e) {
					log.log(Level.SEVERE, "Exception", e);
				}
				chunkCount++;
				hashEntryList.clear();
			}
		}
	}

}
