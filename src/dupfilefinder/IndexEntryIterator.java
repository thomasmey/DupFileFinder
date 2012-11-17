/*
 * Copyright 2012 Thomas Meyer
 */

package dupfilefinder;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

// Map.Entry<String, Long>
// Key=Long, Value=String, Entry=FileEntry<Key,Value>
public class IndexEntryIterator<K, V, E extends Map.Entry<K,V>> implements Iterator<Map.Entry<K, List<V>>> {

	private Queue<E> entryQueue;
	private K currentKey;
	private ObjectInputStream ois;

	public IndexEntryIterator(File sortFileName) throws IOException, ClassNotFoundException {

		// open output stream for iterator
		ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(sortFileName)));

		entryQueue = new LinkedList<E>();

		E entry = null;
		try {
			entry = (E) ois.readObject();
		} catch (EOFException e) {
		}
		if(entry != null) {
			currentKey = entry.getKey();
			entryQueue.add(entry);
		}
	}

	@Override
	public boolean hasNext() {
		return entryQueue.peek() != null;
	}

	@Override
	public Map.Entry<K, List<V>> next() {

		E entry;
		List<V> currentValueList = new ArrayList<V>();

		while((entry=entryQueue.peek()) != null) {

			// gruppenwechsel
			if(!currentKey.equals(entry.getKey())) {
				Map.Entry<K, List<V>> ent =  new AbstractMap.SimpleEntry<K, List<V>>(currentKey, currentValueList);
				currentKey = (K) entry.getKey();
				return ent;
			}
			entryQueue.remove();

			// put next object on list
			if(ois!=null) {
				E fe = null;
				try {
					fe = (E) ois.readObject();
				} catch (EOFException e) {
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
				if(fe != null)
					entryQueue.add(fe);
			}

			currentValueList.add(entry.getValue());
		}

		Map.Entry<K, List<V>> ent = new AbstractMap.SimpleEntry<K, List<V>>(currentKey, currentValueList);
		return ent;

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
