package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.pipeline.FutureList;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.ImmediateValue;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.IntValuedSetting;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;
import com.google.appengine.tools.pipeline.JobSetting.WaitForSetting;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.impl.FutureValueImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class JobRecord extends CascadeModelObject implements JobInfo {

  public static enum State {
    WAITING_FOR_RUN_SLOTS,
    READY_TO_RUN,
    WAITING_FOR_FINALIZE_SLOT,
    READY_TO_FINALIZE,
    FINALIZED,
    STOPPED,
    RETRY
  }

  public static final String DATA_STORE_KIND = "job";
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

  // persistent
  private Key jobInstanceKey;
  private Key runBarrierKey;
  private Key finalizeBarrierKey;
  private Key outputSlotKey;
  private State state;
  private String errorMessage;
  private Date startTime;
  private Date endTime;
  private List<Key> childKeys;
  private long attemptNumber = 0;
  private long maxAttempts = JobSetting.MaxAttempts.DEFAULT;
  private long backoffSeconds = JobSetting.BackoffSeconds.DEFAULT;
  private long backoffFactor = JobSetting.BackoffFactor.DEFAULT;

  // transient
  private Barrier runBarrierInflated;
  private Barrier finalizeBarrierInflated;
  private Slot outputSlotInflated;
  private JobInstanceRecord jobInstanceRecordInflated;

  @SuppressWarnings("unchecked")
  public JobRecord(Entity entity) {
    super(entity);
    this.jobInstanceKey = (Key) entity.getProperty(JOB_INSTANCE_PROPERTY);
    this.finalizeBarrierKey = (Key) entity.getProperty(FINALIZE_BARRIER_PROPERTY);
    this.runBarrierKey = (Key) entity.getProperty(RUN_BARRIER_PROPERTY);
    this.outputSlotKey = (Key) entity.getProperty(OUTPUT_SLOT_PROPERTY);
    this.state = State.valueOf((String) entity.getProperty(STATE_PROPERTY));
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
  }

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
    return entity;
  }

  @SuppressWarnings("unchecked")
  public JobRecord(JobSetting[] jobSettings, Key rootJobKey, Job<?> jobInstance, Object... params) {
    super(rootJobKey);
    rootJobKey = this.rootJobKey;
    this.jobInstanceRecordInflated = new JobInstanceRecord(this, jobInstance);
    this.jobInstanceKey = this.jobInstanceRecordInflated.key;
    state = State.WAITING_FOR_RUN_SLOTS;
    runBarrierInflated = new Barrier(Barrier.Type.RUN, this);
    runBarrierKey = runBarrierInflated.key;
    for (Object param : params) {
      if (null == param || !(param instanceof Value<?>)) {
        param = new ImmediateValue(param);
      }
      if (param instanceof ImmediateValue<?>) {
        ImmediateValue<?> iv = (ImmediateValue<?>) param;

        Slot slot = new Slot(rootJobKey);
        slot.fill(iv.getValue());
        runBarrierInflated.addRegularArgumentSlot(slot);
      } else if (param instanceof FutureList<?>) {
        FutureList<?> futureList = (FutureList<?>) param;
        List<Slot> slotList = new ArrayList<Slot>(futureList.getListOfValues().size());
        for (Value<?> val : futureList.getListOfValues()) {
          Slot slot = null;
          if (val instanceof FutureValueImpl){
            FutureValueImpl<?> futureParam = (FutureValueImpl<?>) val;
            slot = futureParam.getSlot();
          }
          else if (val instanceof ImmediateValue){
            ImmediateValue<?> iv = (ImmediateValue<?>) val;
            slot = new Slot(rootJobKey);
            slot.fill(iv.getValue());
          }
          else{
            throw new RuntimeException("Unrecognized implementation of Value interface: " + val.getClass());
          }
          slotList.add(slot);
        }
        runBarrierInflated.addListArgumentSlots(slotList);
      } else if (param instanceof FutureValue<?>) {
        FutureValueImpl<?> futureParam = (FutureValueImpl<?>) param;
        Slot slot = futureParam.getSlot();
        runBarrierInflated.addRegularArgumentSlot(slot);
      } else {
        throw new RuntimeException(
            "Internal logic error. param class=" + param.getClass().getName());
      }
    }
    for (JobSetting setting : jobSettings) {
      if (setting instanceof WaitForSetting) {
        WaitForSetting wf = (WaitForSetting) setting;
        FutureValueImpl fv = (FutureValueImpl) wf.getFutureValue();
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
          throw new RuntimeException(
              "Unrecognized JobOption class " + setting.getClass().getName());
        }
      } else {
        throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
      }
    }
    if (0 == runBarrierInflated.getWaitingOnKeys().size()) {
      // If the run barrier is not waiting on anything, add a phantom filled
      // slot
      // in order to trigger a HandleSlotFilledTask in order to trigger
      // a RunJobTask.
      Slot slot = new Slot(rootJobKey);
      slot.fill(null);
      runBarrierInflated.addPhantomArgumentSlot(slot);
    }
    finalizeBarrierInflated = new Barrier(Barrier.Type.FINALIZE, this);
    finalizeBarrierKey = finalizeBarrierInflated.key;
    outputSlotInflated = new Slot(rootJobKey);
    // Initially we set the filler of the output slot to be this Job.
    // During finalize we may reset it to the filler of the finalize slot.
    outputSlotInflated.setSourceJobKey(this.getKey());
    outputSlotKey = outputSlotInflated.key;
    childKeys = new LinkedList<Key>();
  }

  @Override
  public String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  private static boolean checkForInflate(CascadeModelObject object, Key expectedGuid, String name) {
    if (null == object) {
      return false;
    }
    if (!expectedGuid.equals(object.getKey())) {
      throw new IllegalArgumentException(
          "Wrong guid for " + name + ". Expected " + expectedGuid + " but was " + object.getKey());
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

  public void appendChildKey(Key key) {
    childKeys.add(key);
  }

  public List<Key> getChildKeys() {
    return childKeys;
  }

  public void setErrorMessage(String message) {
    this.errorMessage = message;
  }

  // Interface JobInfo
  public JobInfo.State getJobState() {
    switch (state) {
      case WAITING_FOR_RUN_SLOTS:
      case READY_TO_RUN:
      case WAITING_FOR_FINALIZE_SLOT:
      case READY_TO_FINALIZE:
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

  public Object getOutput() {
    if (null == outputSlotInflated) {
      return null;
    } else {
      return outputSlotInflated.getValue();
    }
  }

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