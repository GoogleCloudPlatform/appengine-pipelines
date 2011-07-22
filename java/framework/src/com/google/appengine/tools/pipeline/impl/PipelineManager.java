package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.FutureList;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.ImmediateValue;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.CascadeBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord.State;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.model.SlotDescriptor;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.FinalizeJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleSlotFilledTask;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PipelineManager {

  private static final Logger logger = Logger.getLogger(PipelineManager.class.getName());

  private static CascadeBackEnd backEnd = new AppEngineBackEnd();

  public static String startNewCascade(JobSetting[] settings, Job<?> jobInstance, Object... params) {
    UpdateSpec updateSpec = new UpdateSpec();
    JobRecord jobRecord = registerNewJob(updateSpec, settings, null, jobInstance, params);
    backEnd.save(updateSpec);
    return KeyFactory.keyToString(jobRecord.getKey());
  }

  public static JobRecord registerNewJob(UpdateSpec updateSpec, JobSetting[] settings,
      JobRecord parentJob, Job<?> jobInstance, Object[] params) {
    Key rootKey = (null == parentJob ? null : parentJob.getRootJobKey());
    JobRecord jobRecord = new JobRecord(settings, rootKey, jobInstance, updateSpec, params);
    if (null != parentJob) {
      parentJob.appendChildKey(jobRecord.getKey());
    }
    Barrier runBarrier = jobRecord.getRunBarrierInflated();
    updateSpec.includeBarrier(runBarrier);
    for (SlotDescriptor descriptor : runBarrier.getWaitingOnInflated()) {
      updateSpec.includeSlot(descriptor.slot);
    }
    updateSpec.includeBarrier(jobRecord.getFinalizeBarrierInflated());
    // Note: the finalize barrier's waiting list has not slots yet.
    updateSpec.includeSlot(jobRecord.getOutputSlotInflated());
    updateSpec.includeJob(jobRecord);
    updateSpec.includeJobInstanceRecord(jobRecord.getJobInstanceInflated());
    return jobRecord;
  }

  /**
   * Given a {@code Value} and a {@code Barrier}, we add one or more slots to
   * the waitingFor list of the barrier corresponding to the {@code Value}. If
   * the value is an {@code ImmediateValue} we make a new slot, register it as
   * filled and add it. If the value is a {@code FutureValue} we add the slot
   * wrapped by the {@code FutreValueImpl}. If the value is a {@code FutureList}
   * then we add multiple slots, one for each {@code Value} in the List wrapped
   * by the {@code FutureList}. This process is not recursive because we do not
   * currently support {@code FutureLists} of {@code FutureLists}.
   * 
   * @param updateSpec
   * @param value
   *          A {@code Value}. {@code Null} is interpreted as an
   *          {@code ImmediateValue} with a value of {@code Null}.
   * @param rootJobKey
   * @param barrier
   */
  public static void registerSlotsWithBarrier(UpdateSpec updateSpec, Value<?> value,
      Key rootJobKey, Barrier barrier) {
    if (null == value || value instanceof ImmediateValue<?>) {
      Object concreteValue = null;
      if (null != value) {
        ImmediateValue<?> iv = (ImmediateValue<?>) value;
        concreteValue = iv.getValue();
      }
      Slot slot = new Slot(rootJobKey);
      registerSlotFilled(updateSpec, slot, concreteValue);
      barrier.addRegularArgumentSlot(slot);
    } else if (value instanceof FutureValueImpl<?>) {
      FutureValueImpl<?> futureValue = (FutureValueImpl<?>) value;
      Slot slot = futureValue.getSlot();
      barrier.addRegularArgumentSlot(slot);
      updateSpec.includeSlot(slot);
    } else if (value instanceof FutureList<?>) {
      FutureList<?> futureList = (FutureList<?>) value;
      List<Slot> slotList = new ArrayList<Slot>(futureList.getListOfValues().size());
      for (Value<?> valFromList : futureList.getListOfValues()) {
        Slot slot = null;
        if (valFromList instanceof ImmediateValue) {
          ImmediateValue<?> ivFromList = (ImmediateValue<?>) valFromList;
          slot = new Slot(rootJobKey);
          registerSlotFilled(updateSpec, slot, ivFromList.getValue());
        } else if (valFromList instanceof FutureValueImpl) {
          FutureValueImpl<?> futureValFromList = (FutureValueImpl<?>) valFromList;
          slot = futureValFromList.getSlot();
        } else if (value instanceof FutureList<?>) {
          throw new IllegalArgumentException(
              "The Pipeline framework does not currently support FutureLists of FutureLists");
        } else {
          throwUnrecognizedValueException(valFromList);
        }
        slotList.add(slot);
        updateSpec.includeSlot(slot);
      }
      barrier.addListArgumentSlots(slotList);
    } else {
      throwUnrecognizedValueException(value);
    }
    updateSpec.includeBarrier(barrier);
  }

  private static void throwUnrecognizedValueException(Value<?> value) {
    throw new RuntimeException(
        "Internal logic error: Unrecognized implementation of Value interface: "
            + value.getClass().getName());
  }

  public static void registerSlotFilled(UpdateSpec updateSpec, Slot slot, Object value) {
    slot.fill(value);
    updateSpec.includeSlot(slot);
    registerSlotFilledTask(updateSpec, slot);
  }

  public static void registerSlotFilledTask(UpdateSpec updateSpec, Slot slot) {
    updateSpec.registerTask(new HandleSlotFilledTask(slot));
  }

  public static PipelineObjects queryFullPipeline(String rootJobHandle) {
    Key rootJobKey = KeyFactory.stringToKey(rootJobHandle);
    return backEnd.queryFullPipeline(rootJobKey);
  }

  private static void checkNonEmpty(String s, String name) {
    if (null == s || s.trim().length() == 0) {
      throw new IllegalArgumentException(name + " is empty.");
    }
  }

  public static JobRecord getJob(String jobHandle) throws NoSuchObjectException {
    checkNonEmpty(jobHandle, "jobHandle");
    Key key = KeyFactory.stringToKey(jobHandle);
    logger.finest("getJob: " + key.getName());
    return backEnd.queryJob(key, false, true);
  }

  public static void stopJob(String jobHandle) throws NoSuchObjectException {
    checkNonEmpty(jobHandle, "jobHandle");
    Key key = KeyFactory.stringToKey(jobHandle);
    JobRecord jobRecord = backEnd.queryJob(key, false, false);
    jobRecord.setState(JobRecord.State.STOPPED);
    UpdateSpec updateSpec = new UpdateSpec();
    updateSpec.includeJob(jobRecord);
    backEnd.save(updateSpec);
  }

  public static void deletePipelineRecords(String pipelineHandle) throws NoSuchObjectException,
      IllegalStateException {
    checkNonEmpty(pipelineHandle, "pipelineHandle");
    Key key = KeyFactory.stringToKey(pipelineHandle);
    backEnd.deletePipeline(key);
  }

  public static void acceptPromisedValue(String promiseHandle, Object value)
      throws NoSuchObjectException {
    checkNonEmpty(promiseHandle, "promiseHandle");
    Key key = KeyFactory.stringToKey(promiseHandle);
    Slot slot = null;
    // It is possible, though unlikely, that we might be asked to accept a
    // promise
    // before the slot to hold the promise has been saved. We will try 5 times,
    // sleeping 1, 2, 4, 8 seconds between attempts.
    for (int i = 0; i < 5; i++) {
      try {
        slot = backEnd.querySlot(key, false);
      } catch (NoSuchObjectException e) {
        try {
          Thread.sleep(((long) Math.pow(2.0, i)) * 1000L);
        } catch (InterruptedException f) {
          // ignore
        }
      }
    }
    if (null == slot) {
      throw new NoSuchObjectException("There is no promise with handle " + promiseHandle);
    }
    UpdateSpec updateSpec = new UpdateSpec();
    registerSlotFilled(updateSpec, slot, value);
    backEnd.save(updateSpec);
  }

  public static void save(UpdateSpec updateSpec) {
    backEnd.save(updateSpec);
  }

  public static void processTask(Task task) {
    logger.finest("Processing task " + task);
    switch (task.getType()) {
    case RUN_JOB:
      RunJobTask runJobTask = (RunJobTask) task;
      runJob(runJobTask.getJobKey());
      break;
    case HANDLE_SLOT_FILLED:
      HandleSlotFilledTask hsfTask = (HandleSlotFilledTask) task;
      handleSlotFilled(hsfTask.getSlotKey());
      break;
    case FINALIZE_JOB:
      FinalizeJobTask finalizeJobTask = (FinalizeJobTask) task;
      finalizeJob(finalizeJobTask.getJobKey());
      break;
    case FAN_OUT:
      FanoutTask fanoutTask = (FanoutTask) task;
      backEnd.handleFanoutTask(fanoutTask);
      break;
    default:
      throw new IllegalArgumentException("Unrecognized task type: " + task.getType());
    }
  }

  public static CascadeBackEnd getBackEnd() {
    return backEnd;
  }

  @SuppressWarnings("unchecked")
  private static void invokePrivateJobMethod(String methodName, Job<?> jobObject, Object... params) {
    Class<?>[] signature = new Class<?>[params.length];
    int i = 0;
    for (Object param : params) {
      signature[i++] = param.getClass();
    }
    invokePrivateJobMethod(methodName, jobObject, signature, params);
  }

  @SuppressWarnings("unchecked")
  private static void invokePrivateJobMethod(String methodName, Job<?> jobObject,
      Class[] signature, Object... params) {
    Class<Job> jobClass = Job.class;
    try {
      Method method = jobClass.getDeclaredMethod(methodName, signature);
      method.setAccessible(true);
      method.invoke(jobObject, params);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  // Currently we simply use the first method named "run" that we find.
  // As long as the user follows the rules and declares a single public method
  // named "run" this will be fine.
  // TODO(rudominer) Consider actually looking for a method with matching
  // signature.
  private static Method findAppropriateRunMethod(Class<?> klass, Object... params) {
    Method runMethod = null;
    for (Method method : klass.getMethods()) {
      if ("run".equals(method.getName())) {
        runMethod = method;
        break;
      }
    }
    return runMethod;
  }

  private static void setJobRecord(Job<?> jobObject, JobRecord jobRecord) {
    invokePrivateJobMethod("setJobRecord", jobObject, jobRecord);
  }

  private static void setUpdateSpec(Job<?> jobObject, UpdateSpec updateSpec) {
    invokePrivateJobMethod("setUpdateSpec", jobObject, updateSpec);
  }

  private static void registerReturnValue(Job<?> jobObject, Value<?> returnValue) {
    invokePrivateJobMethod("registerReturnValue", jobObject, new Class[] { Value.class },
        returnValue);
  }

  private static void runJob(Key jobKey) {
    JobRecord jobRecord = null;
    try {
      jobRecord = backEnd.queryJob(jobKey, true, false);
    } catch (NoSuchObjectException e) {
      throw new RuntimeException("Cannot find some part of the job: " + jobKey, e);
    }
    Key rootJobKey = jobRecord.getRootJobKey();
    JobRecord rootJobRecord = jobRecord;
    if (!rootJobKey.equals(jobKey)) {
      try {
        rootJobRecord = backEnd.queryJob(rootJobKey, false, false);
      } catch (NoSuchObjectException f) {
        throw new RuntimeException("Cannot find root job: " + rootJobKey, f);
      }
    }
    if (rootJobRecord.getState() == JobRecord.State.STOPPED) {
      logger.warning("The pipeline has been stopped: " + rootJobRecord);
      return;
    }
    JobRecord.State jobState = jobRecord.getState();
    if (JobRecord.State.READY_TO_RUN != jobState && JobRecord.State.RETRY != jobState) {
      logger.warning("Not READY_TO_RUN " + jobRecord);
      return;
    }
    Barrier runBarrier = jobRecord.getRunBarrierInflated();
    if (null == runBarrier) {
      throw new RuntimeException("" + jobRecord + " has not been inflated.");
    }
    JobInstanceRecord record = jobRecord.getJobInstanceInflated();
    if (null == record) {
      throw new RuntimeException("" + jobRecord + " does not have jobInstanceInflated.");
    }
    Job<?> jobObject = record.getJobInstanceDeserialized();
    Object[] params = runBarrier.buildArgumentArray();
    setJobRecord(jobObject, jobRecord);
    jobRecord.incrementAttemptNumber();
    UpdateSpec updateSpec = new UpdateSpec();
    setUpdateSpec(jobObject, updateSpec);
    Method runMethod = findAppropriateRunMethod(jobObject.getClass(), params);
    if (logger.isLoggable(Level.FINEST)) {
      StringBuilder builder = new StringBuilder(1024);
      builder.append("Running " + jobRecord + " with params: ");
      builder.append(StringUtils.toString(params));
      logger.finest(builder.toString());
    }
    // TODO(rudominer) We currently don't save the job record until after the
    // job runs.
    // So we are not saving the job start time until after the job runs.
    jobRecord.setStartTime(new Date());
    Value<?> returnValue = null;
    Exception caughtException = null;
    try {
      runMethod.setAccessible(true);
      returnValue = (Value<?>) runMethod.invoke(jobObject, params);
    } catch (Exception e) {
      caughtException = e;
    }
    if (null != caughtException) {
      // Make a new UpdateSpec. Any changes made to the UpdateSpec during
      // the failed Job should be discarded.
      updateSpec = new UpdateSpec();
      handleExceptionDuringRun(jobRecord, rootJobRecord, updateSpec, caughtException);
    } else {
      logger.finest("Job returned: " + returnValue);
      registerReturnValue(jobObject, returnValue);
      jobRecord.setState(State.WAITING_FOR_FINALIZE_SLOT);
    }
    updateSpec.includeJob(jobRecord);
    save(updateSpec);
  }

  private static void handleExceptionDuringRun(JobRecord jobRecord, JobRecord rootJobRecord,
      UpdateSpec updateSpec, Exception e) {
    int attemptNumber = jobRecord.getAttemptNumber();
    int maxAttempts = jobRecord.getMaxAttempts();
    String message = StringUtils.printStackTraceToString(e);
    jobRecord.setErrorMessage(message);
    Key thisJobKey = jobRecord.getKey();
    if (attemptNumber >= maxAttempts) {
      jobRecord.setState(JobRecord.State.STOPPED);
      rootJobRecord.setState(JobRecord.State.STOPPED);
      rootJobRecord.setErrorMessage(message);
      updateSpec.includeJob(rootJobRecord);
    } else {
      jobRecord.setState(JobRecord.State.RETRY);
      Task task = new RunJobTask(thisJobKey, null);
      int backoffFactor = jobRecord.getBackoffFactor();
      int backoffSeconds = jobRecord.getBackoffSeconds();
      task.setDelaySeconds(backoffSeconds * (long) Math.pow(backoffFactor, attemptNumber));
      updateSpec.registerTask(task);
    }
    logger.log(Level.SEVERE, "An exception occurred while attempting to run " + jobRecord + ". "
        + "This was attempt number " + attemptNumber + " of " + maxAttempts + ".", e);
  }

  private static void finalizeJob(Key jobKey) {
    JobRecord jobRecord = null;
    try {
      jobRecord = backEnd.queryJob(jobKey, false, true);
      if (JobRecord.State.READY_TO_FINALIZE != jobRecord.getState()) {
        logger.warning("Not READ_TO_FINALIZE: " + jobRecord);
        return;
      }
      Barrier finalizeBarrier = jobRecord.getFinalizeBarrierInflated();
      if (null == finalizeBarrier) {
        throw new RuntimeException("" + jobRecord + " has not been inflated");
      }
      List<Object> finalizeArguments = finalizeBarrier.buildArgumentList();
      int numFinalizeArguments = finalizeArguments.size();
      if (1 != numFinalizeArguments) {
        throw new RuntimeException("Internal logic error: numFinalizeArguments=" + numFinalizeArguments);
      }
      Object finalizeValue = finalizeArguments.get(0);
      logger.finest("Finalizing " + jobRecord + " with value=" + finalizeValue);
      Slot slot = jobRecord.getOutputSlotInflated();
      if (null == slot) {
        throw new RuntimeException("" + jobRecord + " has not been inflated.");
      }
      UpdateSpec updateSpec = new UpdateSpec();
      registerSlotFilled(updateSpec, slot, finalizeValue);
      jobRecord.setState(JobRecord.State.FINALIZED);
      jobRecord.setEndTime(new Date());
      updateSpec.includeJob(jobRecord);
      backEnd.save(updateSpec);
    } catch (Exception e) {
      logger.log(Level.WARNING, "An exception occurred while attempting to finalize " + jobRecord,
          e);
      throw new RuntimeException(e);
    }
  }

  private static void handleSlotFilled(Key slotKey) {
    Slot slot = null;
    try {
      slot = backEnd.querySlot(slotKey, true);
    } catch (NoSuchObjectException e) {
      throw new RuntimeException("No slot found with key=" + slotKey, e);
    }
    List<Barrier> waitingList = slot.getWaitingOnMeInflated();
    if (null == waitingList) {
      throw new RuntimeException("Internal logic error: " + slot + " is not inflated");
    }
    // For each barrier that is waiting on the slot ...
    for (Barrier barrier : waitingList) {
      logger.finest("Checking " + barrier);
      // unless the barrier has already been released,
      if (!barrier.isReleased()) {
        // we check whether the barrier should be released.
        boolean shouldBeReleased = true;
        if (null == barrier.getWaitingOnInflated()) {
          throw new RuntimeException("Internal logic error: " + barrier + " is not inflated.");
        }
        // For each slot that the barrier is waiting on...
        for (SlotDescriptor sd : barrier.getWaitingOnInflated()) {
          // see if it is full.
          if (!sd.slot.isFilled()) {
            logger.finest("Not filled: " + sd.slot);
            shouldBeReleased = false;
            break;
          }
        }
        if (shouldBeReleased) {
          releaseBarrier(barrier);
        }
      }
    }
  }

  private static void releaseBarrier(Barrier barrier) {
    logger.finest("Releasing " + barrier);
    Key jobKey = barrier.getJobKey();
    JobRecord job = null;
    try {
      job = backEnd.queryJob(jobKey, false, false);
    } catch (NoSuchObjectException e) {
      throw new RuntimeException(e);
    }
    Task task;
    JobRecord.State jobState;
    switch (barrier.getType()) {
    case RUN:
      String taskName = null;
      task = new RunJobTask(jobKey, taskName);
      jobState = JobRecord.State.READY_TO_RUN;
      break;
    case FINALIZE:
      taskName = null;
      task = new FinalizeJobTask(jobKey, taskName);
      jobState = JobRecord.State.READY_TO_FINALIZE;
      break;
    default:
      throw new RuntimeException("Unknown barrier type " + barrier.getType());
    }
    backEnd.releaseBarrier(barrier, job, jobState, task);
  }

}
