package dupfilefinder;

import java.io.Serializable;

public class SortEntry<K extends Comparable<K>,V> implements Comparable<SortEntry<K,V>>, Serializable {
	
	protected K key;
	protected V value;
	
	/*
	 * on disk version 1
	 */
	private static final long serialVersionUID = 1L;
	
	public SortEntry(K key, V value) {
		super();
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	@Override
	public int compareTo(SortEntry<K, V> o) {
		return o.getKey().compareTo(getKey());
	}
}
