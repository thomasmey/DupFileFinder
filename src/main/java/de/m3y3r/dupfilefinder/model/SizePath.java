package de.m3y3r.dupfilefinder.model;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SizePath implements Serializable {

	/* version 1 of this object */
	private static final long serialVersionUID = 1L;

	private final long fileSize;
	private final CharSequence filePath;

	public SizePath(long fileSize, CharSequence filePath) {
		this.fileSize = fileSize;
		this.filePath = filePath;
	}

	public long getFileSize() {
		return fileSize;
	}

	public CharSequence getFilePath() {
		return filePath;
	}
//
//	private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
//		firstName = aInputStream.readUTF();
//		lastName = aInputStream.readUTF();
//		accountNumber = aInputStream.readInt();
//		dateOpened = new Date(aInputStream.readLong());
//	}
//
//	private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
//		aOutputStream.writeInt(accountNumber);
//		aOutputStream.writeLong(dateOpened.getTime());
//	}
}
