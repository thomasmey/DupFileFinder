/*
 * Copyright 2012 Thomas Meyer
 */

package jobcontrol;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

public class JobController {

	private final int maxJobWorker;
	private Queue<Job> jobQueue  = new LinkedList<Job>();
	private JobWorker[] jobThreads;
	private Logger log;

	public JobController(Logger log, int maxParallelJobs) {

		this.maxJobWorker = maxParallelJobs;
		this.log = log;

		jobThreads = new JobWorker[maxJobWorker];
		for(int i = 0; i< jobThreads.length; i++) {
			jobThreads[i] = new JobWorker(log, jobQueue);
			jobThreads[i].setName("JobWorker-" +i);
			jobThreads[i].start();
		}
	}

	public boolean submitJob(Job job) throws InterruptedException {

		synchronized (jobQueue) {
			jobQueue.offer(job);
			jobQueue.notify();
		}

		return true;
	}
	
	public void waitForEmptyQueue() {
		
		// wait for request to finish
		while(!jobQueue.isEmpty()) {
			synchronized (jobQueue) {
				jobQueue.notifyAll();
			}
			// give threads a chance to grab the lock
		}
	}

	public void finishClass(Class clazz, Object... args) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, InterruptedException {

		// jobQueue must be empty!
		assert(jobQueue.size()==0);

		// create common job data -> call clazz.finish(Object... args);
		JobCommon jc = new JobCommon(clazz, new Class[] {}, new Object[] {}, "finish", new Class[] {}, true);
		Job j = new Job(jc); 
		j.setMainMethodParameters();
		synchronized (jobQueue) {
			// submit a job for each thread
			for(Thread t: jobThreads)
				jobQueue.offer(j);
			jobQueue.notifyAll();
		}

		// wait for finish() to finish
		waitForEmptyQueue();
	}

	public void stopWorkers() {
		
		// stop all workers
		for(JobWorker r: jobThreads) {
			r.setRunning(false);
		}

		// kick worker threads out of wait loop -> end them
		synchronized (jobQueue) {
			jobQueue.notifyAll();
		}
	}
}
