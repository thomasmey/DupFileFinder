package de.m3y3r.dupfilefinder;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.ObjectOutputStream;

import common.io.index.IndexWriter;

public class JavaSerialIndexWriter<T> implements IndexWriter<T>, Flushable, Closeable {

	private ObjectOutputStream oos;

	public JavaSerialIndexWriter(File indexFile) throws IOException {
		oos = new ObjectOutputStream(new FileOutputStream(indexFile));
	}

	@Override
	public void writeObject(T obj) throws IOException {
		oos.writeObject(obj);
	}

	@Override
	public void flush() throws IOException {
		oos.flush();
	}

	@Override
	public void close() throws IOException {
		if(oos != null) {
			oos.close();
		}
	}
}
