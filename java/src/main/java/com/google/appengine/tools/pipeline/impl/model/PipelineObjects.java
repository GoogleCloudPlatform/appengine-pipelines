// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.api.datastore.Key;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A container for holding the results of querying for all objects associated
 * with a given root Job.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class PipelineObjects {

  private static final Logger log = Logger.getLogger(PipelineObjects.class.getName());

  public JobRecord rootJob;
  public Map<Key, JobRecord> jobs;
  public Map<Key, Slot> slots;
  public Map<Key, Barrier> barriers;
  public Map<Key, JobInstanceRecord> jobInstanceRecords;

  /**
   * The {@code PipelineObjects} takes ownership of the objects passed in. The
   * caller should not hold references to them.
   */
  public PipelineObjects(Key rootJobKey, Map<Key, JobRecord> jobs, Map<Key, Slot> slots,
      Map<Key, Barrier> barriers, Map<Key, JobInstanceRecord> jobInstanceRecords,
      Map<Key, ExceptionRecord> failureRecords) {
    this.jobInstanceRecords = jobInstanceRecords;
    this.barriers = barriers;
    this.jobs = jobs;
    this.slots = slots;
    Map<Key, String> jobToChildGuid = new HashMap<>();
    for (JobRecord job : jobs.values()) {
      jobToChildGuid.put(job.getKey(), job.getChildGraphGuid());
      if (job.getKey().equals(rootJobKey)) {
        this.rootJob = job;
      }
    }
    for (Iterator<JobRecord> iter = jobs.values().iterator(); iter.hasNext(); ) {
      JobRecord job = iter.next();
      if (job != rootJob) {
        Key parentKey = job.getGeneratorJobKey();
        String graphGuid = job.getGraphGuid();
        if (parentKey == null || graphGuid == null) {
          log.info("Ignoring a non root job with no parent or graphGuid -> " + job);
          iter.remove();
        } else if (!graphGuid.equals(jobToChildGuid.get(parentKey))) {
          log.info("Ignoring an orphand job " + job + ", parent: " + jobs.get(parentKey));
          iter.remove();
        }
      }
    }
    if (null == rootJob) {
      throw new IllegalArgumentException(
          "None of the jobs were the root job with key " + rootJobKey);
    }
    for (Iterator<Slot> iter = slots.values().iterator(); iter.hasNext(); ) {
      Slot slot = iter.next();
      Key parentKey = slot.getGeneratorJobKey();
      String parentGuid = slot.getGraphGuid();
      if (parentKey == null && parentGuid == null
          || parentGuid != null && parentGuid.equals(jobToChildGuid.get(parentKey))) {
        slot.inflate(barriers);
      } else {
        log.info("Ignoring an orphand slot " + slot + ", parent: " + jobs.get(parentKey));
        iter.remove();
      }
    }
    for (Iterator<Barrier> iter = barriers.values().iterator(); iter.hasNext(); ) {
      Barrier barrier = iter.next();
      Key parentKey = barrier.getGeneratorJobKey();
      String parentGuid = barrier.getGraphGuid();
      if (parentKey == null && parentGuid == null
          || parentGuid != null && parentGuid.equals(jobToChildGuid.get(parentKey))) {
        barrier.inflate(slots);
      } else {
        log.info("Ignoring an orphand Barrier " + barrier + ", parent: " + jobs.get(parentKey));
        iter.remove();
      }
    }
    for (JobRecord jobRec : jobs.values()) {
      Barrier runBarrier = barriers.get(jobRec.getRunBarrierKey());
      Barrier finalizeBarrier = barriers.get(jobRec.getFinalizeBarrierKey());
      Slot outputSlot = slots.get(jobRec.getOutputSlotKey());
      JobInstanceRecord jobInstanceRecord = jobInstanceRecords.get(jobRec.getJobInstanceKey());
      ExceptionRecord failureRecord = null;
      Key failureKey = jobRec.getExceptionKey();
      if (null != failureKey) {
        failureRecord = failureRecords.get(failureKey);
      }
      jobRec.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
    }
  }
}
