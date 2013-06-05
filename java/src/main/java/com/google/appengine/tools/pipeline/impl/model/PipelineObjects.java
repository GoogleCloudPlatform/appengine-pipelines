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

import java.util.Map;

/**
 * A container for holding the results of querying for all objects associated
 * with a given root Job.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class PipelineObjects {

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
    for (JobRecord job : jobs.values()) {
      if (job.getKey().equals(rootJobKey)) {
        this.rootJob = job;
      }
    }
    if (null == rootJob) {
      throw new IllegalArgumentException(
          "None of the jobs were the root job with key " + rootJobKey);
    }
    for (Slot slot : slots.values()) {
      slot.inflate(barriers);
    }
    for (Barrier barrier : barriers.values()) {
      barrier.inflate(slots);
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
