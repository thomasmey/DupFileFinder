package dupfilefinder;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class SerializedMerger<T extends SortEntry<?, ?>> {

	private final int noMaxObjects;

	private ObjectInputStream[] ois;
	private File[] inFile;
	private ObjectOutputStream oos;

	public SerializedMerger(final String sortFileName, int noMaxObjects) throws FileNotFoundException, IOException {

		if(noMaxObjects <=0)
			throw new IllegalArgumentException("noMaxArguments is <=0");

		if(sortFileName == null)
			throw new IllegalArgumentException("fileName is null");

		if(sortFileName != null && sortFileName.isEmpty())
			throw new IllegalArgumentException("fileName is empty!");

		// get working directory
		File workDir = new File(System.getProperty("user.dir"));
		File[] files = workDir.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File path, String name) {
					return name.startsWith(sortFileName + ".");
				}
			}
		);


		int noChunks = files.length;
		ois    = new ObjectInputStream[noChunks];
		inFile = new File[noChunks];
		for(int i=0; i< noChunks;i++) {
			inFile[i] = files[i];
			ois[i] = new ObjectInputStream(new FileInputStream(inFile[i]));
		}
		
		oos = new ObjectOutputStream(new FileOutputStream(sortFileName));
		this.noMaxObjects = noMaxObjects;
	}

	void mergeSortedFiles() throws IOException, ClassNotFoundException {
		assert(oos!=null);
		assert(ois!=null);
		for(int i=0; i < ois.length;i++)
			assert(ois[i]!=null);
		assert(noMaxObjects > 0);

		if(ois.length==0)
			return;

		Queue<T> q = new PriorityQueue<T>();
		T objr = null;
		T objw;
		List<T> objc = new ArrayList<T>(ois.length); // current object value
		for (int i=0 ; i < ois.length; i++)
			objc.add(objr);

		boolean[] eof = new boolean[ois.length];
		boolean eofAll = false;

		T objcomp = null;
		T objprev = null;
		int j,i;

		while (!eofAll) {

			objprev=null;
			// find smallest obj
			for(i=0, j=0; j< ois.length; j++) {

				if(!eof[j]) {
					objcomp = objc.get(j);
					if(objcomp==null) {
						i=j;
						break;
					} else {
						if(objprev==null) {
							objprev = objcomp;
							i=j;
						}

						if( objcomp.compareTo(objprev)<0) {
							objprev = objcomp;
							i=j;
						}
					}
				}
			}

			// read object
			if(eof[i])
				continue;

			try {
				objr = (T) ois[i].readObject();
			} catch(EOFException e) {
				// end of input
				objr = null;
			}

			if (objr != null) {
				objc.set(i, objr);
				q.offer(objr);
			} else {
				eof[i] = true;
				objc.set(i, null);
				eofAll = eof[i];
				for(boolean b: eof)
					eofAll &= b;
			}

			if (q.size() > noMaxObjects || eofAll) {
				objw=q.peek();
				for(;objw!=null;) {
					if(objprev.compareTo(objw)<=0)
						break;
					q.remove();
					oos.writeObject(objw);
					objw=q.peek();
				}						
			}
		}
	}

	public void close(boolean removeInputfiles) throws IOException {

		for(int i=0; i < ois.length;i++) {
			ois[i].close();
			if(removeInputfiles)
				inFile[i].delete();
		}
		oos.close();
	}
}
