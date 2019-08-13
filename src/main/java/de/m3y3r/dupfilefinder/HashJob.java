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

import common.io.index.impl.SortingIndexWriter;
import de.m3y3r.dupfilefinder.model.HashString;
import de.m3y3r.dupfilefinder.model.SizePath;

public class HashJob implements Runnable {

	private final SizePath fileEntry;

	private final static Logger log = Logger.getLogger(HashJob.class.getName());
	private final static String algo = "SHA-1";

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

	private SortingIndexWriter<HashString> writer;

	public HashJob(SizePath fileEntry, SortingIndexWriter<HashString> writer) {
		this.fileEntry = fileEntry;
		this.writer = writer;
		HashController.incJobCount();
	}

	public void run() {
		try {
			String hashString = null;
			if(fileEntry.getFileSize() > 0) {
				Path fp = Paths.get(fileEntry.getFilePath());
				if(fp.toFile().exists() && fp.toFile().length() == fileEntry.getFileSize()) {
					hashString = calculateHash(Paths.get(fileEntry.getFilePath()));
				} else {
					log.log(Level.SEVERE, "File was removed or size did change {0}", fp);
					return;
				}
			} else {
				hashString = "00";
			}

			if(hashString == null) {
				throw new IllegalArgumentException();
			}

			// HashString -> (FileName, FileSize)
			HashString he = new HashString(hashString, fileEntry.getFilePath(), fileEntry.getFileSize());
			synchronized (writer) {
				try {
					writer.writeObject(he);
				} catch (IOException e) {
					log.log(Level.SEVERE, "Error!", e);
				}
			}
		} finally {
			HashController.decJobCount();
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
