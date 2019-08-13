package de.m3y3r.dupfilefinder.model;  

public class HashString {
	private final java.lang.CharSequence hashString;
	private final java.lang.CharSequence filePath;
	private final long fileSize;

	public HashString(CharSequence hashString, CharSequence filePath, long fileSize) {
		super();
		this.hashString = hashString;
		this.filePath = filePath;
		this.fileSize = fileSize;
	}

	public java.lang.CharSequence getHashString() {
		return hashString;
	}

	public java.lang.CharSequence getFilePath() {
		return filePath;
	}

	public long getFileSize() {
		return fileSize;
	}
}
