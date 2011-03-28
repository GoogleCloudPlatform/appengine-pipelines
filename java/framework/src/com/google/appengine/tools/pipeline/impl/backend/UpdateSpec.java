// Copyright 2010 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.CascadeModelObject;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class UpdateSpec {
	
	private static final int INITIAL_SIZE = 20;
	
	private Set<Task> taskSet = new HashSet<Task>(INITIAL_SIZE);
	private Map<Key, JobRecord> jobMap = new HashMap<Key, JobRecord>(INITIAL_SIZE);
	private Map<Key, Barrier> barrierMap = new HashMap<Key, Barrier>(INITIAL_SIZE);
	private Map<Key, Slot> slotMap = new HashMap<Key, Slot>(INITIAL_SIZE);
	private Map<Key, JobInstanceRecord> jobInstanceMap = new HashMap<Key, JobInstanceRecord>(INITIAL_SIZE);
	
	private static <E extends CascadeModelObject> void put(Map<Key, E> map, E object){
		map.put(object.getKey(),object);
	}

	public void registerTask(Task task) {
		taskSet.add(task);
	}
	
	public Collection<Task> getTasks(){
		return taskSet;
	}

	public void includeBarrier(Barrier barrier) {
		put(barrierMap, barrier);
	}
	
	public Collection<Barrier> getBarriers(){
		return barrierMap.values();
	}

	public void includeJob(JobRecord job) {
		put(jobMap, job);
	}
	
	public Collection<JobRecord> getJobs(){
		return jobMap.values();
	}

	public void includeSlot(Slot slot) {
		put(slotMap, slot);
	}
	
	public Collection<Slot> getSlots(){
		return slotMap.values();
	}
	
	public void includeJobInstanceRecord(JobInstanceRecord record){
	  put(jobInstanceMap, record);
	}
	
	public Collection<JobInstanceRecord> getJobInstanceRecords() {
	  return jobInstanceMap.values();
	}
}
