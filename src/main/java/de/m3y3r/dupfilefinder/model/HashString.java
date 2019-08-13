package de.m3y3r.dupfilefinder.model;

import java.io.Serializable;

public class HashString implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String hashString;
	private final String filePath;
	private final long fileSize;

	public HashString(String hashString, String filePath, long fileSize) {
		this.hashString = hashString;
		this.filePath = filePath;
		this.fileSize = fileSize;
	}

	public String getHashString() {
		return hashString;
	}

	public String getFilePath() {
		return filePath;
	}

	public long getFileSize() {
		return fileSize;
	}
}
