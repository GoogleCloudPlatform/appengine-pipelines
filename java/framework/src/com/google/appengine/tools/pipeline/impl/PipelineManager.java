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

package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.tools.pipeline.FutureList;
import com.google.appengine.tools.pipeline.ImmediateValue;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.CascadeBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec.Group;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord.State;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.model.SlotDescriptor;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.FinalizeJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleSlotFilledTask;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The central hub of the Pipeline implementation.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class PipelineManager {

  private static final Logger logger = Logger.getLogger(PipelineManager.class.getName());

  private static CascadeBackEnd backEnd = new AppEngineBackEnd();

  /**
   * Creates and launches a new Pipeline
   * <p>
   * Creates the root Job with its associated Barriers and Slots and saves them
   * to the data store. All slots in the root job are immediately filled with
   * the values of the parameters to this method, and
   * {@link HandleSlotFilledTask HandleSlotFilledTasks} are enqueued for each of
   * the filled slots.
   *
   * @param settings JobSettings used to control details of the Pipeline
   * @param jobInstance A user-supplied instance of {@link Job} that will serve
   *        as the root job of the Pipeline.
   * @param params Arguments to the root job's run() method
   * @return The pipelineID of the newly created pipeline, also known as the
   *         rootJobID.
   */
  public static String startNewPipeline(
      JobSetting[] settings, Job<?> jobInstance, Object... params) {
    UpdateSpec updateSpec = new UpdateSpec(null);
    String graphGUID = null; // The root job graph doesn't have a GUID
    JobRecord parentJobKey = null; // The root job graph doesn't have a parent
    // Create the root Job and its associated Barriers and Slots
    // Create HandleSlotFilledTasks for the input parameters.
    JobRecord jobRecord =
        registerNewJobRecord(updateSpec, settings, parentJobKey, graphGUID, jobInstance, params);
    updateSpec.setRootJobKey(jobRecord.getRootJobKey());
    // Save the Pipeline model objects and enqueue the tasks that start the
    // Pipeline executing.
    backEnd.save(updateSpec);
    return KeyFactory.keyToString(jobRecord.getKey());
  }

  /**
   * Creates a new JobRecord with its associated Barriers and Slots. Also
   * creates new {@link HandleSlotFilledTask} for any inputs to the Job that are
   * immediately specified. Registers all newly created objects with the
   * provided {@code UpdateSpec} for later saving.
   * <p>
   * This method is called when starting a new Pipeline, in which case it is
   * used to create the root job, and it is called from within the run() method
   * of a generator job in order to create a child job.
   *
   * @param updateSpec The {@code UpdateSpec} with which to register all newly
   *        created objects. All objects will be added to the
   *        {@link UpdateSpec#getNonTransactionalGroup() non-transaction group}
   *        of the {@code UpdateSpec}.
   * @param settings Array of {@code JobSettings} to apply to the newly created
   *        JobRecord.
   * @param generatorJob The generator job or {@code null} if we are creating
   *        the root job.
   * @param graphGUID The GUID of the child graph to which the new Job belongs
   *        or {@code null} if we are creating the root job.
   * @param jobInstance The user-supplied instance of {@code Job} that
   *        implements the Job that the newly created JobRecord represents.
   * @param params The arguments to be passed to the run() method of the newly
   *        created Job. Each argument may be an actual value or it may be an
   *        object of type {@link Value} representing either an
   *        {@link ImmediateValue} or a
   *        {@link com.google.appengine.tools.pipeline.FutureValue FutureValue}.
   *        For each element of the array, if the Object is not of type
   *        {@link Value} then it is interpreted as an {@link ImmediateValue}
   *        with the given Object as its value.
   * @return The newly constructed JobRecord.
   */
  @SuppressWarnings("unchecked")
  public static JobRecord registerNewJobRecord(UpdateSpec updateSpec, JobSetting[] settings,
      JobRecord generatorJob, String graphGUID, Job<?> jobInstance, Object[] params) {
    Key rootKey = (null == generatorJob ? null : generatorJob.getRootJobKey());
    Key generatorKey = (null == generatorJob ? null : generatorJob.getKey());

    JobRecord jobRecord = new JobRecord(rootKey, generatorKey, graphGUID, jobInstance, settings);

    updateSpec.setRootJobKey(jobRecord.getRootJobKey());

    // Add slots to the RunBarrier corresponding to the input parameters
    for (Object param : params) {
      Value<?> value;
      if (null != param && param instanceof Value<?>) {
        value = (Value<?>) param;
      } else {
        value = new ImmediateValue<Object>(param);
      }
      registerSlotsWithBarrier(updateSpec, value, jobRecord.getRootJobKey(), generatorKey,
          graphGUID, jobRecord.getRunBarrierInflated());
    }

    if (0 == jobRecord.getRunBarrierInflated().getWaitingOnKeys().size()) {
      // If the run barrier is not waiting on anything, add a phantom filled
      // slot in order to trigger a HandleSlotFilledTask in order to trigger
      // a RunJobTask.
      Slot slot = new Slot(jobRecord.getRootJobKey(), generatorKey, graphGUID);
      jobRecord.getRunBarrierInflated().addPhantomArgumentSlot(slot);
      registerSlotFilled(updateSpec, slot, null);
    }

    // Register the newly created objects with the UpdateSpec.
    // The slots in the run Barrier have already been registered
    // and the finalize Barrier doesn't have any slots yet.
    // Any HandleSlotFilledTasks have also been registered already.
    Group updateGroup = updateSpec.getNonTransactionalGroup();
    updateGroup.includeBarrier(jobRecord.getRunBarrierInflated());
    updateGroup.includeBarrier(jobRecord.getFinalizeBarrierInflated());
    updateGroup.includeSlot(jobRecord.getOutputSlotInflated());
    updateGroup.includeJob(jobRecord);
    updateGroup.includeJobInstanceRecord(jobRecord.getJobInstanceInflated());

    return jobRecord;
  }


  /**
   * Given a {@code Value} and a {@code Barrier}, we add one or more slots to
   * the waitingFor list of the barrier corresponding to the {@code Value}. We
   * also create {@link HandleSlotFilledTask HandleSlotFilledTasks} for all
   * filled slots. We register all newly created Slots and Tasks with the given
   * {@code UpdateSpec}.
   * <p>
   * If the value is an {@code ImmediateValue} we make a new slot, register it
   * as filled and add it. If the value is a {@code FutureValue} we add the slot
   * wrapped by the {@code FutreValueImpl}. If the value is a {@code FutureList}
   * then we add multiple slots, one for each {@code Value} in the List wrapped
   * by the {@code FutureList}. This process is not recursive because we do not
   * currently support {@code FutureLists} of {@code FutureLists}.
   *
   * @param updateSpec All newly created Slots will be added to the
   *        {@link UpdateSpec#getNonTransactionalGroup() non-transactional
   *        group} of the updateSpec. All {@link HandleSlotFilledTask
   *        HandleSlotFilledTasks} created will be added to the
   *        {@link UpdateSpec#getFinalTransaction() final transaction}. Note
   *        that {@code barrier} will not be added to updateSpec. That must be
   *        done by the caller.
   * @param value A {@code Value}. {@code Null} is interpreted as an
   *        {@code ImmediateValue} with a value of {@code Null}.
   * @param rootJobKey The rootJobKey of the Pipeline in which the given Barrier
   *        lives.
   * @param generatorJobKey The key of the generator Job of the local graph in
   *        which the given barrier lives, or {@code null} if the barrier lives
   *        in the root Job graph.
   * @param graphGUID The GUID of the local graph in which the barrier lives, or
   *        {@code null} if the barrier lives in the root Job graph.
   * @param barrier The barrier to which we will add the slots
   */
  private static void registerSlotsWithBarrier(UpdateSpec updateSpec, Value<?> value,
      Key rootJobKey, Key generatorJobKey, String graphGUID, Barrier barrier) {
    if (null == value || value instanceof ImmediateValue<?>) {
      Object concreteValue = null;
      if (null != value) {
        ImmediateValue<?> iv = (ImmediateValue<?>) value;
        concreteValue = iv.getValue();
      }
      Slot slot = new Slot(rootJobKey, generatorJobKey, graphGUID);
      registerSlotFilled(updateSpec, slot, concreteValue);
      barrier.addRegularArgumentSlot(slot);
    } else if (value instanceof FutureValueImpl<?>) {
      FutureValueImpl<?> futureValue = (FutureValueImpl<?>) value;
      Slot slot = futureValue.getSlot();
      barrier.addRegularArgumentSlot(slot);
      updateSpec.getNonTransactionalGroup().includeSlot(slot);
    } else if (value instanceof FutureList<?>) {
      FutureList<?> futureList = (FutureList<?>) value;
      List<Slot> slotList = new ArrayList<Slot>(futureList.getListOfValues().size());
      // The dummyListSlot is a marker slot that indicates that the
      // next group of slots forms a single list argument.
      Slot dummyListSlot = new Slot(rootJobKey, generatorJobKey, graphGUID);
      registerSlotFilled(updateSpec, dummyListSlot, null);
      for (Value<?> valFromList : futureList.getListOfValues()) {
        Slot slot = null;
        if (valFromList instanceof ImmediateValue<?>) {
          ImmediateValue<?> ivFromList = (ImmediateValue<?>) valFromList;
          slot = new Slot(rootJobKey, generatorJobKey, graphGUID);
          registerSlotFilled(updateSpec, slot, ivFromList.getValue());
        } else if (valFromList instanceof FutureValueImpl<?>) {
          FutureValueImpl<?> futureValFromList = (FutureValueImpl<?>) valFromList;
          slot = futureValFromList.getSlot();
        } else if (value instanceof FutureList<?>) {
          throw new IllegalArgumentException(
              "The Pipeline framework does not currently support FutureLists of FutureLists");
        } else {
          throwUnrecognizedValueException(valFromList);
        }
        slotList.add(slot);
        updateSpec.getNonTransactionalGroup().includeSlot(slot);
      }
      barrier.addListArgumentSlots(dummyListSlot, slotList);
    } else {
      throwUnrecognizedValueException(value);
    }
  }

  private static void throwUnrecognizedValueException(Value<?> value) {
    throw new RuntimeException(
        "Internal logic error: Unrecognized implementation of Value interface: "
        + value.getClass().getName());
  }

  /**
   * Given a Slot and a concrete value with which to fill the slot, we fill the
   * value with the slot and create a new {@link HandleSlotFilledTask} for the
   * newly filled Slot. We register the Slot and the Task with the given
   * UpdateSpec for later saving.
   *
   * @param updateSpec The Slot will be added to the
   *        {@link UpdateSpec#getNonTransactionalGroup() non-transactional
   *        group} of the updateSpec. The new {@link HandleSlotFilledTask} will
   *        be added to the {@link UpdateSpec#getFinalTransaction() final
   *        transaction}.
   * @param slot the Slot to fill
   * @param value the value with which to fill it
   */
  private static void registerSlotFilled(UpdateSpec updateSpec, Slot slot, Object value) {
    slot.fill(value);
    updateSpec.getNonTransactionalGroup().includeSlot(slot);
    updateSpec.getFinalTransaction().registerTask(new HandleSlotFilledTask(slot));
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

  /**
   * Retrieves a JobRecord for the specified job handle. The returned instance
   * will be only partially inflated. The run and finalize barriers will not be
   * inflated by the output slot will be.
   *
   * @param jobHandle The handle of a job.
   * @return The corresponding JobRecord
   * @throws NoSuchObjectException If a JobRecord with the given handle cannot
   *         be found in the data store.
   */
  public static JobRecord getJob(String jobHandle) throws NoSuchObjectException {
    checkNonEmpty(jobHandle, "jobHandle");
    Key key = KeyFactory.stringToKey(jobHandle);
    logger.finest("getJob: " + key.getName());
    return backEnd.queryJob(key, JobRecord.InflationType.FOR_OUTPUT);
  }

  /**
   * Changes the state of the specified job to STOPPED.
   *
   * @param jobHandle The handle of a job
   * @throws NoSuchObjectException If a JobRecord with the given handle cannot
   *         be found in the data store.
   */
  public static void stopJob(String jobHandle) throws NoSuchObjectException {
    checkNonEmpty(jobHandle, "jobHandle");
    Key key = KeyFactory.stringToKey(jobHandle);
    JobRecord jobRecord = backEnd.queryJob(key, JobRecord.InflationType.NONE);
    jobRecord.setState(JobRecord.State.STOPPED);
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    updateSpec.getTransaction("stopJob").includeJob(jobRecord);
    backEnd.save(updateSpec);
  }

  /**
   * Delete all data store entities corresponding to the given pipeline.
   *
   * @param pipelineHandle The handle of the pipeline to be deleted
   * @param force If this parameter is not {@code true} then this method will
   *        throw an {@link IllegalStateException} if the specified pipeline is
   *        not in the
   *        {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED}
   *        or
   *        {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED}
   *        state.
   * @param async If this parameter is {@code true} then instead of performing
   *        the delete operation synchronously, this method will enqueue a task
   *        to perform the operation.
   * @throws NoSuchObjectException If there is no Job with the given key.
   * @throws IllegalStateException If {@code force = false} and the specified
   *         pipeline is not in the
   *         {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED}
   *         or
   *         {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED}
   *         state.
   */
  public static void deletePipelineRecords(String pipelineHandle, boolean force, boolean async)
      throws NoSuchObjectException, IllegalStateException {
    checkNonEmpty(pipelineHandle, "pipelineHandle");
    Key key = KeyFactory.stringToKey(pipelineHandle);
    backEnd.deletePipeline(key, force, async);
  }

  public static void acceptPromisedValue(String promiseHandle, Object value)
      throws NoSuchObjectException, OrphanedObjectException {
    checkNonEmpty(promiseHandle, "promiseHandle");
    Key key = KeyFactory.stringToKey(promiseHandle);
    Slot slot = null;
    // It is possible, though unlikely, that we might be asked to accept a
    // promise before the slot to hold the promise has been saved. We will try 5
    // times, sleeping 1, 2, 4, 8 seconds between attempts.
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
    Key generatorJobKey = slot.getGeneratorJobKey();
    if (null == generatorJobKey) {
      throw new RuntimeException(
          "Pipeline is fatally corrupted. Slot for promised value has no generatorJobKey: "
          + slot);
    }
    JobRecord generatorJob = backEnd.queryJob(generatorJobKey, JobRecord.InflationType.NONE);
    if (null == generatorJob) {
      throw new RuntimeException("Pipeline is fatally corrupted. "
          + "The generator job for a promised value slot was not found: " + generatorJobKey);
    }
    String childGraphGuid = generatorJob.getChildGraphGuid();
    if (null == childGraphGuid) {
      // The generator job has not been saved with a childGraphGuid yet. This
      // can happen if
      // the promise handle leaked out to an external thread before the job that
      // generated it
      // had finished.
      throw new NoSuchObjectException(
          "The framework is not ready to accept the promised value yet. "
          + "Please try again after the job that generated the promis handle has completed.");
    }
    if (!childGraphGuid.equals(slot.getGraphGuid())) {
      // The slot has been orphaned
      throw new OrphanedObjectException(promiseHandle);
    }
    UpdateSpec updateSpec = new UpdateSpec(slot.getRootJobKey());
    registerSlotFilled(updateSpec, slot, value);
    backEnd.save(updateSpec);
  }

  /**
   * A RuntimeException which, when thrown, causes us to abandon the current
   * task, by returning a 200.
   */
  private static class AbandonTaskException extends RuntimeException {
  }

  /**
   * Process an incoming task received from the App Engine task queue.
   *
   * @param task The task to be processed.
   */
  public static void processTask(Task task) {
    logger.finest("Processing task " + task);
    try {
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
          handleFanoutTaskOrAbandonTask(fanoutTask);
          break;
      case DELETE_PIPELINE:
          DeletePipelineTask deletePipelineTask = (DeletePipelineTask) task;
          try {
            backEnd.deletePipeline(
                deletePipelineTask.getRootJobKey(), deletePipelineTask.shouldForce(), false);
          } catch (Exception e) {
            logger.log(Level.WARNING, "DeletePipeline operation failed.", e);
          }
          break;
      default:
          throw new IllegalArgumentException("Unrecognized task type: " + task.getType());
      }
    } catch (AbandonTaskException ate) {
      // return 200;
    }
  }

  public static CascadeBackEnd getBackEnd() {
    return backEnd;
  }

  @SuppressWarnings("unchecked")
  private static void invokePrivateJobMethod(
      String methodName, Job<?> jobObject, Object... params) {
    Class<?>[] signature = new Class<?>[params.length];
    int i = 0;
    for (Object param : params) {
      signature[i++] = param.getClass();
    }
    invokePrivateJobMethod(methodName, jobObject, signature, params);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void invokePrivateJobMethod(String methodName, Job<?> jobObject, Class<
      ?>[] signature, Object... params) {
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
  // TODO(user) Consider actually looking for a method with matching
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

  private static void setCurrentRunGuid(Job<?> jobObject, String guid) {
    invokePrivateJobMethod("setCurrentRunGuid", jobObject, guid);
  }

  private static void setUpdateSpec(Job<?> jobObject, UpdateSpec updateSpec) {
    invokePrivateJobMethod("setUpdateSpec", jobObject, updateSpec);
  }

  /**
   * Run the job with the given key.
   * <p>
   * We fetch the {@link JobRecord} from the data store and then fetch its run
   * {@link Barrier} and all of the {@link Slot Slots} in the run
   * {@code Barrier} (which should be filled.) We use the values of the filled
   * {@code Slots} to populate the arguments of the run() method of the
   * {@link Job} instance associated with the job and we invoke the
   * {@code run()} method. We save any generated child job graph and we enqueue
   * a {@link HandleSlotFilledTask} for any slots that are filled immediately.
   *
   * @see "http://goto/java-pipeline-model"
   *
   * @param jobKey
   */
  private static void runJob(Key jobKey) {
    JobRecord jobRecord = null;
    jobRecord = queryJobOrAbandonTask(jobKey, JobRecord.InflationType.FOR_RUN);
    Key rootJobKey = jobRecord.getRootJobKey();
    JobRecord rootJobRecord = jobRecord;
    if (!rootJobKey.equals(jobKey)) {
      rootJobRecord = queryJobOrAbandonTask(rootJobKey, JobRecord.InflationType.NONE);
    }
    if (rootJobRecord.getState() == JobRecord.State.STOPPED) {
      logger.warning("The pipeline has been stopped: " + rootJobRecord);
      throw new AbandonTaskException();
    }
    JobRecord.State jobState = jobRecord.getState();
    Barrier runBarrier = jobRecord.getRunBarrierInflated();
    if (null == runBarrier) {
      throw new RuntimeException("Internal logic error: " + jobRecord
          + " has not been inflated.");
    }
    Barrier finalizeBarrier = jobRecord.getFinalizeBarrierInflated();
    if (null == finalizeBarrier) {
      throw new RuntimeException("Internal logic error: finalize barrier not inflated in "
          + jobRecord);
    }

    // Release the run barrier now so any concurrent HandleSlotFilled tasks
    // will stop trying to release it.
    runBarrier.setReleased();
    UpdateSpec updateSpec = new UpdateSpec(rootJobKey);
    updateSpec.getTransaction("releaseRunBarrier").includeBarrier(runBarrier);
    backEnd.save(updateSpec);
    updateSpec = new UpdateSpec(rootJobKey);

    switch (jobState) {
    case WAITING_TO_RUN:
    case RETRY:
        // OK, proceed
        break;
    case WAITING_TO_FINALIZE:
        logger.info("This job has already been run " + jobRecord);
        return;
    case STOPPED:
        logger.info("This job has been stoped. " + jobRecord);
        return;
    }

    // Deserialize the instance of Job and set some values on the instance
    JobInstanceRecord record = jobRecord.getJobInstanceInflated();
    if (null == record) {
      throw new RuntimeException("Internal logic error:" + jobRecord
          + " does not have jobInstanceInflated.");
    }
    Job<?> jobObject = record.getJobInstanceDeserialized();
    setJobRecord(jobObject, jobRecord);
    String currentRunGUID = GUIDGenerator.nextGUID();
    setCurrentRunGuid(jobObject, currentRunGUID);
    setUpdateSpec(jobObject, updateSpec);

    // Get the run() method we will invoke and its arguments
    Object[] params = runBarrier.buildArgumentArray();
    Method runMethod = findAppropriateRunMethod(jobObject.getClass(), params);
    if (logger.isLoggable(Level.FINEST)) {
      StringBuilder builder = new StringBuilder(1024);
      builder.append("Running " + jobRecord + " with params: ");
      builder.append(StringUtils.toString(params));
      logger.finest(builder.toString());
    }

    // Set the Job's start time and save the jobRecord now before we invoke
    // run(). The start time will be displayed in the UI.
    jobRecord.incrementAttemptNumber();
    jobRecord.setStartTime(new Date());
    UpdateSpec tempSpec = new UpdateSpec(jobRecord.getRootJobKey());
    tempSpec.getNonTransactionalGroup().includeJob(jobRecord);
    backEnd.save(tempSpec);

    // Invoke the run() method. This has the side-effect of populating
    // the UpdateSpec with any child job graph generated by the run().
    Value<?> returnValue = null;
    Exception caughtException = null;
    try {
      runMethod.setAccessible(true);
      returnValue = (Value<?>) runMethod.invoke(jobObject, params);
    } catch (Exception e) {
      caughtException = e;
    }
    if (null != caughtException) {
      handleExceptionDuringRun(jobRecord, rootJobRecord, caughtException);
      return;
    }

    // The run() method returned without error.
    // We do all of the following in a transaction:
    // (1) Check that the job is currently in the state WAITING_TO_RUN or RETRY
    // (2) Change the state of the job to WAITING_TO_FINALIZE
    // (3) Set the finalize slot to be the one generated by the run() method
    // (4) Set the job's child graph GUID to be the currentRunGUID
    // (5) Enqueue a FanoutTask that will fan-out to a set of
    // HandleSlotFilledTasks for each of the slots that were immediately filled
    // by the running of the job.
    // See "http://goto/java-pipeline-model".
    logger.finest("Job returned: " + returnValue);
    registerSlotsWithBarrier(updateSpec, returnValue, rootJobKey, jobRecord.getKey(),
        currentRunGUID, finalizeBarrier);
    jobRecord.setState(State.WAITING_TO_FINALIZE);
    jobRecord.setChildGraphGuid(currentRunGUID);
    updateSpec.getFinalTransaction().includeJob(jobRecord);
    updateSpec.getFinalTransaction().includeBarrier(finalizeBarrier);
    backEnd.saveWithJobStateCheck(
        updateSpec, jobKey, JobRecord.State.WAITING_TO_RUN, JobRecord.State.RETRY);
  }

  private static void handleExceptionDuringRun(
      JobRecord jobRecord, JobRecord rootJobRecord, Exception e) {
    int attemptNumber = jobRecord.getAttemptNumber();
    int maxAttempts = jobRecord.getMaxAttempts();
    String message = StringUtils.printStackTraceToString(e);
    jobRecord.setErrorMessage(message);
    Key thisJobKey = jobRecord.getKey();
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    if (attemptNumber >= maxAttempts) {
      jobRecord.setState(JobRecord.State.STOPPED);
      rootJobRecord.setState(JobRecord.State.STOPPED);
      rootJobRecord.setErrorMessage(message);
      updateSpec.getNonTransactionalGroup().includeJob(jobRecord);
      updateSpec.getNonTransactionalGroup().includeJob(rootJobRecord);
      backEnd.save(updateSpec);
    } else {
      jobRecord.setState(JobRecord.State.RETRY);
      updateSpec.getNonTransactionalGroup().includeJob(jobRecord);
      updateSpec.getNonTransactionalGroup().includeJob(rootJobRecord);
      backEnd.save(updateSpec);
      int backoffFactor = jobRecord.getBackoffFactor();
      int backoffSeconds = jobRecord.getBackoffSeconds();
      Task task = new RunJobTask(jobRecord.getKey(), attemptNumber);
      task.setDelaySeconds(backoffSeconds * (long) Math.pow(backoffFactor, attemptNumber));
      backEnd.enqueue(task);
    }
    logger.log(Level.SEVERE, "An exception occurred while attempting to run " + jobRecord + ". "
        + "This was attempt number " + attemptNumber + " of " + maxAttempts + ".", e);

  }

  /**
   * Finalize the job with the given key.
   * <p>
   * We fetch the {@link JobRecord} from the data store and then fetch its
   * finalize {@link Barrier} and all of the {@link Slot Slots} in the finalize
   * {@code Barrier} (which should be filled.) We set the finalize Barrier to
   * released and save it. We fetch the job's output Slot. We use the values of
   * the filled finalize {@code Slots} to populate the output slot. We set the
   * state of the Job to {@code FINALIZED} and save the {@link JobRecord} and
   * the output slot. Finally we enqueue a {@link HandleSlotFilledTask} for the
   * output slot.
   *
   * @see "http://goto/java-pipeline-model"
   *
   * @param jobKey
   */
  private static void finalizeJob(Key jobKey) {
    JobRecord jobRecord = null;

    // Get the JobRecord, its finalize Barrier, all the slots in the
    // finalize Barrier, and the job's output Slot.
    jobRecord = queryJobOrAbandonTask(jobKey, JobRecord.InflationType.FOR_FINALIZE);
    Barrier finalizeBarrier = jobRecord.getFinalizeBarrierInflated();
    if (null == finalizeBarrier) {
      throw new RuntimeException("" + jobRecord + " has not been inflated");
    }
    Slot outputSlot = jobRecord.getOutputSlotInflated();
    if (null == outputSlot) {
      throw new RuntimeException("" + jobRecord + " has not been inflated.");
    }

    // release the finalize barrier now so that any concurrent
    // HandleSlotFilled tasks will stop trying
    finalizeBarrier.setReleased();
    UpdateSpec updateSpec = new UpdateSpec(jobRecord.getRootJobKey());
    updateSpec.getTransaction("releaseFinalizeBarrier").includeBarrier(finalizeBarrier);
    backEnd.save(updateSpec);
    updateSpec = new UpdateSpec(jobRecord.getRootJobKey());

    // Copy the finalize value to the output slot
    List<Object> finalizeArguments = finalizeBarrier.buildArgumentList();
    int numFinalizeArguments = finalizeArguments.size();
    if (1 != numFinalizeArguments) {
      throw new RuntimeException("Internal logic error: numFinalizeArguments="
          + numFinalizeArguments);
    }
    Object finalizeValue = finalizeArguments.get(0);
    logger.finest("Finalizing " + jobRecord + " with value=" + finalizeValue);
    outputSlot.fill(finalizeValue);

    // Change state of the job to FINALIZED and set the end time
    jobRecord.setState(JobRecord.State.FINALIZED);
    jobRecord.setEndTime(new Date());

    // Propagate the filler of the finalize slot to also be the filler of the
    // output slot. If there is no unique filler of the finalize slot then we
    // resort to assigning the current job as the filler job.
    Key fillerJobKey = getFinalizeSlotFiller(finalizeBarrier);
    if (null == fillerJobKey) {
      fillerJobKey = jobKey;
    }
    outputSlot.setSourceJobKey(fillerJobKey);

    // Save the job and the output slot
    updateSpec.getNonTransactionalGroup().includeJob(jobRecord);
    updateSpec.getNonTransactionalGroup().includeSlot(outputSlot);
    backEnd.save(updateSpec);

    // enqueue a HandleSlotFilled task
    backEnd.enqueue(new HandleSlotFilledTask(outputSlot));

  }

  /**
   * Return the unique value of {@link Slot#getSourceJobKey()} for all slots in
   * the finalize Barrier, if there is a unique such value. Otherwise return
   * {@code null}.
   *
   * @param finalizeBarrier A finalize Barrier
   * @return The unique slot filler for the finalize slots, or {@code null}
   */
  private static Key getFinalizeSlotFiller(Barrier finalizeBarrier) {
    Key fillerJobKey = null;
    for (SlotDescriptor slotDescriptor : finalizeBarrier.getWaitingOnInflated()) {
      Key key = slotDescriptor.slot.getSourceJobKey();
      if (null != key) {
        if (null == fillerJobKey) {
          fillerJobKey = key;
        } else {
          if (!fillerJobKey.toString().equals(key.toString())) {
            // Found 2 non-equal values, return null
            return null;
          }
        }
      }
    }
    return fillerJobKey;
  }


  /**
   * Handle the fact that the slot with the given key has been filled.
   * <p>
   * For each barrier that is waiting on the slot, if all of the slots that the
   * barrier is waiting on are now filled then the barrier should be released.
   * Release the barrier by enqueueing an appropriate task (either
   * {@link RunJobTask} or {@link FinalizeJobTask}.
   *
   * @param slotKey The key of the slot that has been filled.
   */
  private static void handleSlotFilled(Key slotKey) {
    Slot slot = null;
    slot = querySlotOrAbandonTask(slotKey, true);
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
          Key jobKey = barrier.getJobKey();
          Task task;
          switch (barrier.getType()) {
          case RUN:
              task = new RunJobTask(jobKey);
              break;
          case FINALIZE:
              task = new FinalizeJobTask(jobKey);
              break;
          default:
              throw new RuntimeException("Unknown barrier type " + barrier.getType());
          }
          try {
            backEnd.enqueue(task);
          } catch (TaskAlreadyExistsException e) {
            // OK. Some other thread has already enqueued this task.
          }
        }
      }
    }
  }

  /**
   * Queries for the job with the given key from the data store and if the job
   * is not found then throws an {@link AbandonTaskException}.
   *
   * @param key The key of the JobRecord to be fetched
   * @param inflationType Specifies the manner in which the returned JobRecord
   *        should be inflated.
   * @return A {@code JobRecord}, possibly with a partially-inflated associated
   *         graph of objects.
   * @throws AbandonTaskException If Either the JobRecord or any of the
   *         associated Slots or Barriers are not found in the data store.
   */
  private static JobRecord queryJobOrAbandonTask(
      Key key, JobRecord.InflationType inflationType) {
    try {
      return backEnd.queryJob(key, inflationType);
    } catch (NoSuchObjectException e) {
      logger.log(Level.SEVERE,
          "Cannot find some part of the job: " + key + ". Aborting the pipeline.", e);
      throw new AbandonTaskException();
    }
  }

  /**
   * Queries the Slot with the given Key from the data store and if the Slot is
   * not found then throws an {@link AbandonTaskException}.
   *
   * @param key The Key of the slot to fetch.
   * @param inflate If this is {@code true} then the Barriers that are waiting
   *        on the Slot and the other Slots that those Barriers are waiting on
   *        will also be fetched from the data store and used to partially
   *        populate the graph of objects attached to the returned Slot. In
   *        particular: {@link Slot#getWaitingOnMeInflated()} will not return
   *        {@code null} and also that for each of the {@link Barrier Barriers}
   *        returned from that method {@link Barrier#getWaitingOnInflated()}
   *        will not return {@code null}.
   * @return A {@code Slot}, possibly with a partially-inflated associated graph
   *         of objects.
   * @throws AbandonTaskException If either the Slot or the associated Barriers
   *         and slots are not found in the data store.
   */
  private static Slot querySlotOrAbandonTask(Key key, boolean inflate) {
    try {
      return backEnd.querySlot(key, inflate);
    } catch (NoSuchObjectException e) {
      logger.log(Level.SEVERE, "Cannot find the slot: " + key + ". Aborting the pipeline.", e);
      throw new AbandonTaskException();
    }
  }

  /**
   * Handles the given FanoutTask and if the corresponding FanoutTaskRecord is
   * not found then throws an {@link AbandonTaskException}.
   *
   * @param fanoutTask The FanoutTask to handle
   */
  private static void handleFanoutTaskOrAbandonTask(FanoutTask fanoutTask) {
    try {
      backEnd.handleFanoutTask(fanoutTask);
    } catch (NoSuchObjectException e) {
      logger.log(Level.SEVERE, "Pipeline is fatally corrupted. Fanout task record not found", e);
      throw new AbandonTaskException();
    }
  }

}
