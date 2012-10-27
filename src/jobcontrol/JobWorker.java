/*
 * Copyright 2012 Thomas Meyer
 */

package jobcontrol;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JobWorker extends Thread {

	// JobController shares this job queue with all workers
	private Queue<Job> queue;
	private Logger log;
	private boolean running = true;
	private HashMap<String,Object> objectMap;

	public JobWorker(Logger log, Queue<Job> queue) {
		this.queue = queue;
		this.log = log;
		objectMap = new HashMap<String, Object>();
	}

	@Override
	public void run() {

		log.log(Level.INFO, "Start thread {0}", Thread.currentThread());

		Job job;

		while(true) {
			synchronized (queue) {
				while(queue.isEmpty() && isRunning())
					try {
						queue.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				if(!isRunning())
					return;
				job = queue.poll();
			}

			// run jobs main method
			Class c = job.getJobCommon().getJobClass();
			Object target;

			// is job's target class poolable?
			if(job.getJobCommon().isPoolable()) {
				// get matching object from object pool
				String classname = c.getName();
				target = objectMap.get(classname);
				if(target == null) {
					target = createTarget(job);
					objectMap.put(classname, target);
				}
			} else {
				// no, so a new target object for each job is created
				// construct new job target object;
				target = createTarget(job);
			}

			if(target != null) {
				// invoke main method
				try {
					Method m = c.getMethod(job.getJobCommon().getMainMethod(), job.getJobCommon().getMainMethodParameterTypes());
					m.invoke(target, job.getMainMethodParameters());
				} catch (Exception e) {
					log.log(Level.SEVERE,"Couldn't invoke job target object main method", e);
				}
			}

		}
	}

	private Object createTarget(Job job) {

		Object ret = null;

		Class c = job.getJobCommon().getJobClass();

		try {
			Constructor constr = c.getConstructor(job.getJobCommon().getConstructorParameterTypes());
			ret = constr.newInstance(job.getJobCommon().getConstructorParameters());
		} catch (Exception e) {
			log.log(Level.SEVERE,"Couldn't create job target object", e);
		}

		return ret;
	}

	public synchronized boolean isRunning() {
		return running;
	}

	public synchronized void setRunning(boolean running) {
		this.running = running;
	}

}
