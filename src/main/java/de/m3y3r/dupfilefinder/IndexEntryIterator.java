/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.avro.specific.SpecificRecord;

import common.io.index.avro.AvroIndexReader;

public class IndexEntryIterator<I extends SpecificRecord> implements Iterator<List<I>> {

	private final Queue<I> entryQueue;
	private final AvroIndexReader<I> indexReader;
	private final Comparator<I> comparator;
	private I currentKey;

	public IndexEntryIterator(File sortFileName, Class<I> clazzIn, Comparator<I> comparator) throws IOException, ClassNotFoundException {

		// open output stream for iterator
		this.indexReader = new AvroIndexReader<I>(clazzIn, sortFileName);
		this.entryQueue = new LinkedList<I>();

		I entry = indexReader.readObject();
		if(entry != null) {
			currentKey = entry;
			entryQueue.add(entry);
		}
		this.comparator = comparator;
	}

	public boolean hasNext() {
		return entryQueue.peek() != null;
	}

	public List<I> next() {

		I entry;
		List<I> currentValueList = new ArrayList<I>();

		entry = entryQueue.peek();
		while(entry != null) {

			// gruppenwechsel
			if(comparator.compare(currentKey, entry) != 0) {
				currentKey = entry;
				return currentValueList;
			}
			entryQueue.remove();

			// put next object on list
			I fe = null;
			try {
				fe = indexReader.readObject();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			if(fe != null)
				entryQueue.add(fe);

			currentValueList.add(fe);
			entry = entryQueue.peek();
		}

		return currentValueList;

	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
