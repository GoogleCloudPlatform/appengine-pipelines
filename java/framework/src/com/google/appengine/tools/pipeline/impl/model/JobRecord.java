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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.IntValuedSetting;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;
import com.google.appengine.tools.pipeline.JobSetting.OnBackend;
import com.google.appengine.tools.pipeline.JobSetting.WaitForSetting;
import com.google.appengine.tools.pipeline.impl.FutureValueImpl;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * The Pipeline model object corresponding to a job.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class JobRecord extends PipelineModelObject implements JobInfo {

  /**
   * 
   * The state of the job.
   * 
   */
  public static enum State {
    WAITING_TO_RUN, WAITING_TO_FINALIZE, FINALIZED, STOPPED, RETRY
  }

  /**
   * This enum serves as an input parameter to the method
   * {@link PipelineBackEnd#queryJob(Key, InflationType)}. When fetching an
   * instance of {@code JobRecord} from the data store this enum specifies how
   * much auxiliary data should also be queried and used to inflate the instance
   * of {@code JobRecord}.
   * 
   */
  public static enum InflationType {
    /**
     * Do not inflate at all
     */
    NONE,

    /**
     * Inflate as necessary to run the job. In particular:
     * <ul>
     * <li>{@link JobRecord#getRunBarrierInflated()} will not return
     * {@code null}; and
     * <li>for the returned {@link Barrier}
     * {@link Barrier#getWaitingOnInflated()} will not return {@code null}; and
     * <li> {@link JobRecord#getOutputSlotInflated()} will not return
     * {@code null}; and
     * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
     * {@code null}
     * </ul>
     */
    FOR_RUN,

    /**
     * Inflate as necessary to finalize the job. In particular:
     * <ul>
     * <li> {@link JobRecord#getOutputSlotInflated()} will not return
     * {@code null}; and
     * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
     * {@code null}; and
     * <li>for the returned {@link Barrier} the method
     * {@link Barrier#getWaitingOnInflated()} will not return {@code null}.
     * </ul>
     */
    FOR_FINALIZE,

    /**
     * Inflate as necessary to retrieve the output of the job. In particular
     * {@link JobRecord#getOutputSlotInflated()} will not return {@code null}
     */
    FOR_OUTPUT;
  }

  public static final String DATA_STORE_KIND = "pipeline-job";
  // Data store entity property names
  private static final String JOB_INSTANCE_PROPERTY = "jobInstance";
  private static final String RUN_BARRIER_PROPERTY = "runBarrier";
  private static final String FINALIZE_BARRIER_PROPERTY = "finalizeBarrier";
  private static final String STATE_PROPERTY = "state";
  private static final String OUTPUT_SLOT_PROPERTY = "outputSlot";
  private static final String ERROR_MESSAGE_PROPERTY = "errorMessage";
  private static final String START_TIME_PROPERTY = "startTime";
  private static final String END_TIME_PROPERTY = "endTime";
  private static final String CHILD_KEYS_PROPERTY = "childKeys";
  private static final String ATTEMPT_NUM_PROPERTY = "attemptNum";
  private static final String MAX_ATTEMPTS_PROPERTY = "maxAttempts";
  private static final String BACKOFF_SECONDS_PROPERTY = "backoffSeconds";
  private static final String BACKOFF_FACTOR_PROPERTY = "backoffFactor";
  private static final String ON_BACKEND_PROPERTY = "onBackend";
  private static final String CHILD_GRAPH_GUID_PROPERTY = "childGraphGuid";

  // persistent fields
  private Key jobInstanceKey;
  private Key runBarrierKey;
  private Key finalizeBarrierKey;
  private Key outputSlotKey;
  private State state;
  private String errorMessage;
  private Date startTime;
  private Date endTime;
  private String childGraphGuid;
  private List<Key> childKeys;
  private long attemptNumber = 0;
  private long maxAttempts = JobSetting.MaxAttempts.DEFAULT;
  private long backoffSeconds = JobSetting.BackoffSeconds.DEFAULT;
  private long backoffFactor = JobSetting.BackoffFactor.DEFAULT;
  private String onBackend = null;

  // transient fields
  private Barrier runBarrierInflated;
  private Barrier finalizeBarrierInflated;
  private Slot outputSlotInflated;
  private JobInstanceRecord jobInstanceRecordInflated;


  /**
   * Re-constitutes an instance of this class from a Data Store entity.
   * 
   * @param entity
   */
  @SuppressWarnings("unchecked")
  public JobRecord(Entity entity) {
    super(entity);
    this.jobInstanceKey = (Key) entity.getProperty(JOB_INSTANCE_PROPERTY);
    this.finalizeBarrierKey = (Key) entity.getProperty(FINALIZE_BARRIER_PROPERTY);
    this.runBarrierKey = (Key) entity.getProperty(RUN_BARRIER_PROPERTY);
    this.outputSlotKey = (Key) entity.getProperty(OUTPUT_SLOT_PROPERTY);
    this.state = State.valueOf((String) entity.getProperty(STATE_PROPERTY));
    Text childGraphGuidText = (Text) entity.getProperty(CHILD_GRAPH_GUID_PROPERTY);
    if (null != childGraphGuidText) {
      this.childGraphGuid = childGraphGuidText.getValue();
    }
    Text errorMessageText = (Text) entity.getProperty(ERROR_MESSAGE_PROPERTY);
    if (null != errorMessageText) {
      this.errorMessage = errorMessageText.getValue();
    }
    this.startTime = (Date) entity.getProperty(START_TIME_PROPERTY);
    this.endTime = (Date) entity.getProperty(END_TIME_PROPERTY);
    this.childKeys = (List<Key>) entity.getProperty(CHILD_KEYS_PROPERTY);
    if (null == childKeys) {
      childKeys = new LinkedList<Key>();
    }
    this.attemptNumber = (Long) entity.getProperty(ATTEMPT_NUM_PROPERTY);
    this.maxAttempts = (Long) entity.getProperty(MAX_ATTEMPTS_PROPERTY);
    this.backoffSeconds = (Long) entity.getProperty(BACKOFF_SECONDS_PROPERTY);
    this.backoffFactor = (Long) entity.getProperty(BACKOFF_FACTOR_PROPERTY);
    this.onBackend = (String) entity.getProperty(ON_BACKEND_PROPERTY);
  }

  /**
   * Constructs and returns a Data Store Entity that represents this model
   * object
   */
  @Override
  public Entity toEntity() {
    Entity entity = toProtoEntity();
    entity.setProperty(JOB_INSTANCE_PROPERTY, jobInstanceKey);
    entity.setProperty(FINALIZE_BARRIER_PROPERTY, finalizeBarrierKey);
    entity.setProperty(RUN_BARRIER_PROPERTY, runBarrierKey);
    entity.setProperty(OUTPUT_SLOT_PROPERTY, outputSlotKey);
    entity.setProperty(STATE_PROPERTY, state.toString());
    if (null != errorMessage) {
      entity.setUnindexedProperty(ERROR_MESSAGE_PROPERTY, new Text(errorMessage));
    }
    if (null != childGraphGuid) {
      entity.setUnindexedProperty(CHILD_GRAPH_GUID_PROPERTY, new Text(childGraphGuid));
    }
    if (null != startTime) {
      entity.setProperty(START_TIME_PROPERTY, startTime);
    }
    if (null != endTime) {
      entity.setProperty(END_TIME_PROPERTY, endTime);
    }
    if (null != childKeys) {
      entity.setProperty(CHILD_KEYS_PROPERTY, childKeys);
    }
    entity.setProperty(ATTEMPT_NUM_PROPERTY, attemptNumber);
    entity.setProperty(MAX_ATTEMPTS_PROPERTY, maxAttempts);
    entity.setProperty(BACKOFF_SECONDS_PROPERTY, backoffSeconds);
    entity.setProperty(BACKOFF_FACTOR_PROPERTY, backoffFactor);
    if (null != onBackend) {
      entity.setProperty(ON_BACKEND_PROPERTY, onBackend);
    }
    return entity;
  }

  /**
   * Constructs a new JobRecord given the provided data. The constructed
   * instance will be inflated in the sense that
   * {@link #getJobInstanceInflated()}, {@link #getFinalizeBarrierInflated()},
   * {@link #getOutputSlotInflated()} and {@link #getRunBarrierInflated()} will
   * all not return {@code null}. This constructor is used when a new JobRecord
   * is created during the run() method of a parent job. The parent job is also
   * known as the generator job.
   * 
   * @param rootJobKeyParam The key of the rootJob of the Pipeline, or
   *        {@code null} if we are constructing the root job.
   * @param generatorJobKeyParam The key of the parent generator job of this
   *        job, or {@code null} if we are constructing the root job.
   * @param graphGUIDParam The GUID of the local graph of this job, or
   *        {@code null} if we are constructing the root job.
   * @param jobInstance The non-null user-supplied instance of {@code Job} that
   *        implements the Job that the newly created JobRecord represents.
   * @param settings Array of {@code JobSettings} to apply to the newly created
   *        JobRecord.
   */
  public JobRecord(Key rootJobKeyParam, Key generatorJobKeyParam, String graphGUIDParam,
      Job<?> jobInstance, JobSetting[] settings) {
    super(rootJobKeyParam, generatorJobKeyParam, graphGUIDParam);
    jobInstanceRecordInflated = new JobInstanceRecord(this, jobInstance);
    jobInstanceKey = jobInstanceRecordInflated.key;
    runBarrierInflated = new Barrier(Barrier.Type.RUN, this);
    runBarrierKey = runBarrierInflated.key;
    finalizeBarrierInflated = new Barrier(Barrier.Type.FINALIZE, this);
    finalizeBarrierKey = finalizeBarrierInflated.key;
    outputSlotInflated = new Slot(rootJobKey, generatorJobKey, graphGUID);
    // Initially we set the filler of the output slot to be this Job.
    // During finalize we may reset it to the filler of the finalize slot.
    outputSlotInflated.setSourceJobKey(key);
    outputSlotKey = outputSlotInflated.key;
    childKeys = new LinkedList<Key>();
    state = State.WAITING_TO_RUN;
    for (JobSetting setting : settings) {
      applySetting(setting);
    }
  }

  private void applySetting(JobSetting setting) {
    if (setting instanceof WaitForSetting) {
      WaitForSetting wf = (WaitForSetting) setting;
      FutureValueImpl<?> fv = (FutureValueImpl<?>) wf.getFutureValue();
      Slot slot = fv.getSlot();
      runBarrierInflated.addPhantomArgumentSlot(slot);
    } else if (setting instanceof IntValuedSetting) {
      int value = ((IntValuedSetting) setting).getValue();
      if (setting instanceof BackoffSeconds) {
        backoffSeconds = value;
      } else if (setting instanceof BackoffFactor) {
        backoffFactor = value;
      } else if (setting instanceof MaxAttempts) {
        maxAttempts = value;
      } else {
        throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
      }
    } else if (setting instanceof OnBackend) {
      onBackend = ((OnBackend) setting).getValue();
    } else {
      throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
    }
  }

  @Override
  public String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  private static boolean checkForInflate(PipelineModelObject object, Key expectedGuid,
      String name) {
    if (null == object) {
      return false;
    }
    if (!expectedGuid.equals(object.getKey())) {
      throw new IllegalArgumentException("Wrong guid for " + name + ". Expected " + expectedGuid
          + " but was " + object.getKey());
    }
    return true;
  }

  public void inflate(Barrier runBarrier, Barrier finalizeBarrier, Slot outputSlot,
      JobInstanceRecord jobInstanceRecord) {
    if (checkForInflate(runBarrier, runBarrierKey, "runBarrier")) {
      runBarrierInflated = runBarrier;
    }
    if (checkForInflate(finalizeBarrier, finalizeBarrierKey, "finalizeBarrier")) {
      finalizeBarrierInflated = finalizeBarrier;
    }
    if (checkForInflate(outputSlot, outputSlotKey, "outputSlot")) {
      outputSlotInflated = outputSlot;
    }
    if (checkForInflate(jobInstanceRecord, jobInstanceKey, "jobInstanceRecord")) {
      this.jobInstanceRecordInflated = jobInstanceRecord;
    }
  }

  public Key getRunBarrierKey() {
    return runBarrierKey;
  }

  public Barrier getRunBarrierInflated() {
    return runBarrierInflated;
  }

  public Key getFinalizeBarrierKey() {
    return finalizeBarrierKey;
  }

  public Barrier getFinalizeBarrierInflated() {
    return finalizeBarrierInflated;
  }

  public Key getOutputSlotKey() {
    return outputSlotKey;
  }

  public Slot getOutputSlotInflated() {
    return outputSlotInflated;
  }

  public Key getJobInstanceKey() {
    return jobInstanceKey;
  }

  public JobInstanceRecord getJobInstanceInflated() {
    return jobInstanceRecordInflated;
  }

  public void setStartTime(Date date) {
    startTime = date;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setEndTime(Date date) {
    endTime = date;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setState(State state) {
    this.state = state;
  }

  public void setChildGraphGuid(String guid) {
    this.childGraphGuid = guid;
  }

  public State getState() {
    return state;
  }

  public int getAttemptNumber() {
    return (int) attemptNumber;
  }

  public void incrementAttemptNumber() {
    attemptNumber++;
  }

  public int getBackoffSeconds() {
    return (int) backoffSeconds;
  }

  public int getBackoffFactor() {
    return (int) backoffFactor;
  }

  public int getMaxAttempts() {
    return (int) maxAttempts;
  }

  public String getOnBackend() {
    return onBackend;
  }

  public void appendChildKey(Key key) {
    childKeys.add(key);
  }

  public List<Key> getChildKeys() {
    return childKeys;
  }

  public String getChildGraphGuid() {
    return childGraphGuid;
  }

  public void setErrorMessage(String message) {
    this.errorMessage = message;
  }

  // Interface JobInfo
  @Override
  public JobInfo.State getJobState() {
    switch (state) {
      case WAITING_TO_RUN:
      case WAITING_TO_FINALIZE:
        return JobInfo.State.RUNNING;
      case FINALIZED:
        return JobInfo.State.COMPLETED_SUCCESSFULLY;
      case STOPPED:
        if (null == errorMessage) {
          return JobInfo.State.STOPPED_BY_REQUEST;
        } else {
          return JobInfo.State.STOPPED_BY_ERROR;
        }
      case RETRY:
        return JobInfo.State.WAITING_TO_RETRY;
      default:
        throw new RuntimeException("Unrecognized state: " + state);
    }
  }

  @Override
  public Object getOutput() {
    if (null == outputSlotInflated) {
      return null;
    } else {
      return outputSlotInflated.getValue();
    }
  }

  @Override
  public String getError() {
    return errorMessage;
  }

  private String getJobInstanceString() {
    if (null == jobInstanceRecordInflated) {
      return "jobInstanceKey=" + jobInstanceKey;
    }
    return jobInstanceRecordInflated.getJobInstanceDeserialized().getClass().getName();
  }

  @Override
  public String toString() {
    return "JobRecord [" + key.getName() + ", " + state + ", " + getJobInstanceString()
        + ", runBarrier=" + runBarrierKey.getName() + ", finalizeBarrier="
        + finalizeBarrierKey.getName() + ", outputSlot=" + outputSlotKey.getName() + "]";
  }

}
