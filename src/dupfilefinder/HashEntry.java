package dupfilefinder;

public class HashEntry extends SortEntry<String,String> {

	private static final long serialVersionUID = 1L;
	private long size;

	public HashEntry(String hashString, String fileName, long size) {
		super(hashString,fileName);
		this.size = size;
	}
}
