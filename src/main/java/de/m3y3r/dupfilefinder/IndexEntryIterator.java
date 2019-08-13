/*
 * Copyright 2012 Thomas Meyer
 */

package de.m3y3r.dupfilefinder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import common.io.index.IndexReader;

public class IndexEntryIterator<I> implements Iterator<List<I>> {

	private final Queue<I> entryQueue;
	private final IndexReader<I> indexReader;
	private final Comparator<I> comparator;
	private I currentKey;

	public IndexEntryIterator(IndexReader<I> indexReader, Comparator<I> comparator) throws IOException, ClassNotFoundException {

		// open output stream for iterator
		this.indexReader = indexReader;
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
			currentValueList.add(entry);

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

			entry = entryQueue.peek();
		}

		return currentValueList;

	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
