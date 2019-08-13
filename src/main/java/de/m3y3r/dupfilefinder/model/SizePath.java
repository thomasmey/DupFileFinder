package de.m3y3r.dupfilefinder.model;

import java.io.Serializable;

public class SizePath implements Serializable {

	/* version 1 of this object */
	private static final long serialVersionUID = 1L;

	private final long fileSize;
	private final String filePath;

	public SizePath(long fileSize, String filePath) {
		this.fileSize = fileSize;
		this.filePath = filePath;
	}

	public long getFileSize() {
		return fileSize;
	}

	public String getFilePath() {
		return filePath;
	}
}
