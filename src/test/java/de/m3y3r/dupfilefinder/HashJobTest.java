package de.m3y3r.dupfilefinder;

import org.junit.Test;

import de.m3y3r.dupfilefinder.model.SizePath;

public class HashJobTest {

	@Test
	public void test() {
		SizePath fileEntry = new SizePath(1, "C:\\Users\\thomas\\Downloads\\eclipse-jee-2018-12-R-win32-x86_64.zip");
		HashJob job = new HashJob(fileEntry, null);
		job.run();
	}
}
