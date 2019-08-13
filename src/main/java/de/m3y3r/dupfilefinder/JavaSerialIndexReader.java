
package de.m3y3r.dupfilefinder;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import common.io.index.IndexReader;

public class JavaSerialIndexReader<T> implements IndexReader<T>, Closeable {

	private ObjectInputStream ois;

	public JavaSerialIndexReader(File indexFile) throws IOException {
		ois = new ObjectInputStream(new FileInputStream(indexFile));
	}

	@Override
	public T readObject() throws IOException {
		try {
			return (T) ois.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		if(ois != null) {
			ois.close();
		}
	}
}
