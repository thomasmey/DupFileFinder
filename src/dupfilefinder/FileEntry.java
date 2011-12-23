package dupfilefinder;

class FileEntry extends SortEntry<Long, String>  {

	/**
	 * on disk version 1
	 */
	private static final long serialVersionUID = 1L;

	public FileEntry(Long key, String value) {
		super(key, value);
	}

	// reverse sort order
	@Override
	public int compareTo(SortEntry<Long, String> o) {
		if (super.compareTo(o) > 0)
				return -1;
		if (super.compareTo(o) < 0)
				return +1;
		return 0;
	}
	
}
