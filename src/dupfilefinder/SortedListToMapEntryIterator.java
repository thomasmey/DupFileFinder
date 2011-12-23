package dupfilefinder;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;

// Key=Long, Value=String, Entry=FileEntry<Key,Value>
public class SortedListToMapEntryIterator<K extends Comparable<K>, V, E extends SortEntry<K,V>> implements Iterator<Entry<K, List<V>>> {

	private Queue<E> entryQueue;
	private K currentKey;
	private ObjectInputStream ois;

	public SortedListToMapEntryIterator(String sortFileName) throws IOException, ClassNotFoundException {

		// open output stream for iterator
		ois = new ObjectInputStream(new FileInputStream(sortFileName));

		entryQueue = new LinkedList<E>();

		E entry = null;
		try {
			entry = (E) ois.readObject();
		} catch (EOFException e) {
		}
		if(entry!=null) {
			currentKey = entry.getKey();
			entryQueue.add(entry);
		}
	}

	@Override
	public boolean hasNext() {
		return entryQueue.peek() != null;
	}

	@Override
	public Entry<K, List<V>> next() {

		E entry;
		List<V> currentValueList = new ArrayList<V>();

		while((entry=entryQueue.peek()) != null) {

			// gruppenwechsel
			if(!currentKey.equals(entry.getKey())) {
				Entry<K, List<V>> ent =  new AbstractMap.SimpleEntry<K, List<V>>(currentKey, currentValueList);
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

		Entry<K, List<V>> ent = new AbstractMap.SimpleEntry<K, List<V>>(currentKey, currentValueList);
		return ent;

	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
	}

}
