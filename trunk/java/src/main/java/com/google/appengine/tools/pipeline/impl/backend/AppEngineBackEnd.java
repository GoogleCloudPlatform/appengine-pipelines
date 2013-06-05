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

package com.google.appengine.tools.pipeline.impl.backend;

import static com.google.appengine.tools.pipeline.impl.util.TestUtils.throwHereForTesting;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineModelObject;
import com.google.appengine.tools.pipeline.impl.model.FanoutTaskRecord;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class AppEngineBackEnd implements PipelineBackEnd {

  private static final Logger logger = Logger.getLogger(AppEngineBackEnd.class.getName());

  // Arbitrary value for now; may need tuning.
  private static final int MAX_ENTITIES_PER_GET = 100;

  private static Random random = new Random();

  private DatastoreService dataStore;
  private AppEngineTaskQueue taskQueue;

  {
    dataStore = DatastoreServiceFactory.getDatastoreService();
    taskQueue = new AppEngineTaskQueue();
  }

  private void putAll(Collection<? extends PipelineModelObject> objects) {
    List<Entity> entityList = new ArrayList<Entity>(objects.size());
    for (PipelineModelObject x : objects) {
      logger.finest("Storing: " + x);
      entityList.add(x.toEntity());
    }
    dataStore.put(entityList);
  }

  private void saveAll(UpdateSpec.Group group) {
    putAll(group.getBarriers());
    putAll(group.getJobs());
    putAll(group.getSlots());
    putAll(group.getJobInstanceRecords());
    putAll(group.getFailureRecords());
  }

  private void transactionallySaveAll(UpdateSpec.Transaction transactionSpec, Key rootJobKey,
      Key jobKey, JobRecord.State... expectedStates) {
    Transaction transaction = dataStore.beginTransaction();
    try {
      if (jobKey != null && expectedStates != null) {
        Entity entity = null;
        try {
          entity = dataStore.get(jobKey);
        } catch (EntityNotFoundException e) {
          throw new RuntimeException(
              "Fatal Pipeline corruption error. No JobRecord found with key = " + jobKey);
        }
        JobRecord jobRecord = new JobRecord(entity);
        JobRecord.State state = jobRecord.getState();
        boolean stateIsExpected = false;
        for (JobRecord.State expectedState : expectedStates) {
          if (state == expectedState) {
            stateIsExpected = true;
            break;
          }
        }
        if (!stateIsExpected) {
          logger.finest("Job " + jobRecord + " is not in one of the expected states: "
              + expectedStates + " and so transactionallySaveAll() will not continue.");
          return;
        }
      }
      saveAll(transactionSpec);
      if (transactionSpec instanceof UpdateSpec.TransactionWithTasks) {
        UpdateSpec.TransactionWithTasks transactionWithTasks =
            (UpdateSpec.TransactionWithTasks) transactionSpec;
        Collection<Task> tasks = transactionWithTasks.getTasks();
        if (tasks.size() > 0) {
          byte[] encodedTasks = FanoutTask.encodeTasks(tasks);
          FanoutTaskRecord ftRecord = new FanoutTaskRecord(rootJobKey, encodedTasks);
          // Store FanoutTaskRecord outside of any transaction, but before
          // the FanoutTask is enqueued. If the put succeeds but the
          // enqueue fails then the FanoutTaskRecord is orphaned. But
          // the Pipeline is still consistent.
          dataStore.put(null, ftRecord.toEntity());
          FanoutTask fannoutTask = new FanoutTask(ftRecord.getKey());
          taskQueue.enqueue(fannoutTask);
        }
      }
      transaction.commit();
    } finally {
      if (transaction.isActive()) {
        transaction.rollback();
      }
    }
  }

  private abstract class Operation {

    private String name;

    Operation(String name) {
      this.name = name;
    }

    public abstract void perform();

    public String getName() {
      return name;
    }
  }

  private void tryFiveTimes(Operation operation) {
    int attempts = 1;
    while (true) {
      try {
        operation.perform();
        return;
      } catch (ConcurrentModificationException e) {
        String logMessage =
            "ConcurrentModificationException during " + operation.getName() + " attempt "
                + attempts + ".";
        if (attempts++ < 5) {
          logger.finest(logMessage + " Trying again.");
          try {
            // Sleep between 0.2 and 4.2 seconds
            Thread.sleep((long) ((random.nextFloat() + 0.05) * 4000.0));
          } catch (InterruptedException f) {
            // ignore
          }
        } else {
          logger.info(logMessage);
          throw e;
        }
      }
    }
  }

  @Override
  public void enqueue(Task task) {
    taskQueue.enqueue(task);
  }

  @Override
  public void saveWithJobStateCheck(final UpdateSpec updateSpec, final Key jobKey,
      final JobRecord.State... expectedStates) {
    tryFiveTimes(new Operation("save") {
      @Override
      public void perform() {
        saveAll(updateSpec.getNonTransactionalGroup());
      }
    });
    for (final UpdateSpec.Transaction transactionSpec : updateSpec.getTransactions()) {
      tryFiveTimes(new Operation("save") {
        @Override
        public void perform() {
          transactionallySaveAll(transactionSpec, updateSpec.getRootJobKey(), null);
        }
      });
    }

    // If a unit test requests us to do so, fail here.
    throwHereForTesting("AppEngineBackeEnd.saveWithJobStateCheck.beforeFinalTransaction");

    tryFiveTimes(new Operation("save") {
      @Override
      public void perform() {
        transactionallySaveAll(updateSpec.getFinalTransaction(), updateSpec.getRootJobKey(),
            jobKey, expectedStates);
      }
    });
  }

  @Override
  public void save(final UpdateSpec updateSpec) {
    saveWithJobStateCheck(updateSpec, null);
  }

  private Entity queryEntity(Key key) throws EntityNotFoundException {
    return dataStore.get(key);
  }

  private Entity transactionallyQueryEntity(Key key) throws EntityNotFoundException {
    Transaction transaction = dataStore.beginTransaction();
    try {
      return queryEntity(key);
    } finally {
      if (transaction.isActive()) {
        transaction.rollback();
      }
    }
  }

  @SuppressWarnings("unused")
  private Map<Key, Entity> transactionallyQueryEntities(Collection<Key> keys) {
    Transaction transaction = dataStore.beginTransaction();
    try {
      return dataStore.get(keys);
    } finally {
      if (transaction.isActive()) {
        transaction.rollback();
      }
    }
  }

  @Override
  public JobRecord queryJob(Key jobKey, JobRecord.InflationType inflationType)
      throws NoSuchObjectException {
    try {
      Entity entity = transactionallyQueryEntity(jobKey);
      JobRecord jobRecord = new JobRecord(entity);
      Barrier runBarrier = null;
      Barrier finalizeBarrier = null;
      Slot outputSlot = null;
      JobInstanceRecord jobInstanceRecord = null;
      ExceptionRecord failureRecord = null;
      switch (inflationType) {
        case FOR_RUN:
          runBarrier = queryBarrier(jobRecord.getRunBarrierKey(), true, true);
          finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), false, true);
          jobInstanceRecord = queryJobInstanceRecord(jobRecord.getJobInstanceKey());
          outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
          break;
        case FOR_FINALIZE:
          finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), true, true);
          outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
          break;
        case FOR_OUTPUT:
          outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
          Key failureKey = jobRecord.getExceptionKey();
          failureRecord = queryFailure(failureKey);
          break;
        default:
      }
      jobRecord.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
      logger.finest("Query returned: " + jobRecord);
      return jobRecord;
    } catch (EntityNotFoundException e) {
      throw new NoSuchObjectException(jobKey.toString(), e);
    }
  }

  /**
   * {@code inflate = true} means that {@link Barrier#getWaitingOnInflated()}
   * will not return {@code null}.
   */
  private Barrier queryBarrier(Key barrierKey, boolean inflate, boolean startNewTransaction)
      throws EntityNotFoundException {
    Entity entity;
    if (startNewTransaction) {
      entity = transactionallyQueryEntity(barrierKey);
    } else {
      entity = queryEntity(barrierKey);
    }
    Barrier barrier = new Barrier(entity);
    if (inflate) {
      Collection<Barrier> barriers = new ArrayList<Barrier>(1);
      barriers.add(barrier);
      inflateBarriers(barriers);
    }
    logger.finest("Querying returned: " + barrier);
    return barrier;
  }

  private JobInstanceRecord queryJobInstanceRecord(Key key) throws EntityNotFoundException {
    Entity entity = queryEntity(key);
    return new JobInstanceRecord(entity);
  }

  /**
   * Given a {@link Collection} of {@link Barrier Barriers}, inflate each of the
   * {@link Barrier Barriers} so that {@link Barrier#getWaitingOnInflated()}
   * will not return null;
   * 
   * @param barriers
   */
  private void inflateBarriers(Collection<Barrier> barriers) {
    // Step 1. Build the set of keys corresponding to the slots.
    Set<Key> keySet = new HashSet<Key>(barriers.size() * 5);
    for (Barrier barrier : barriers) {
      for (Key key : barrier.getWaitingOnKeys()) {
        keySet.add(key);
      }
    }
    // Step 2. Query the datastore for the Slot entities
    Map<Key, Entity> entityMap = dataStore.get(keySet);
    // Step 3. Convert into map from key to Slot
    Map<Key, Slot> slotMap = new HashMap<Key, Slot>(entityMap.size());
    for (Key key : entityMap.keySet()) {
      Slot s = new Slot(entityMap.get(key));
      slotMap.put(key, s);
    }
    // Step 4. Inflate each of the barriers
    for (Barrier barrier : barriers) {
      barrier.inflate(slotMap);
    }
  }

  /**
   * Returns all entities specified by the given keys.
   *
   * @throws NoSuchObjectException if any entities don't exist
   */
  private Map<Key, Entity> getAll(List<Key> keys) throws NoSuchObjectException {
    Map<Key, Entity> out = new HashMap<Key, Entity>(keys.size());
    int start = 0;
    while (start < keys.size()) {
      int end = Math.min(keys.size(), start + MAX_ENTITIES_PER_GET);
      List<Key> batch = keys.subList(start, end);
      Map<Key, Entity> results = dataStore.get(null, batch);
      if (results.size() != batch.size()) {
        List<Key> missing = new ArrayList<Key>(batch);
        missing.removeAll(results.keySet());
        logger.severe("Missing entities for keys: " + missing + " (and perhaps others)");
        throw new NoSuchObjectException("" + missing.get(0));
      }
      out.putAll(results);
      start = end;
    }
    return out;
  }

  @Override
  public Slot querySlot(Key slotKey, boolean inflate) throws NoSuchObjectException {
    Entity entity;
    try {
      entity = transactionallyQueryEntity(slotKey);
    } catch (EntityNotFoundException e) {
      throw new NoSuchObjectException(slotKey.toString(), e);
    }
    Slot slot = new Slot(entity);
    if (inflate) {
      Map<Key, Entity> entities = getAll(slot.getWaitingOnMeKeys());
      Map<Key, Barrier> barriers = new HashMap<Key, Barrier>(entities.size());
      for (Map.Entry<Key, Entity> entry : entities.entrySet()) {
        barriers.put(entry.getKey(), new Barrier(entry.getValue()));
      }
      slot.inflate(barriers);
      inflateBarriers(barriers.values());
    }
    return slot;
  }

  @Override
  public ExceptionRecord queryFailure(Key failureKey) throws NoSuchObjectException {
    if (failureKey == null) {
      return null;
    }
    Entity entity;
    try {
      entity = transactionallyQueryEntity(failureKey);
    } catch (EntityNotFoundException e) {
      throw new NoSuchObjectException(failureKey.toString(), e);
    }
    return new ExceptionRecord(entity);
  }

  @Override
  public Object serlializeValue(Object value) throws IOException {
    // return JsonUtils.toJson(value);
    return new Blob(SerializationUtils.serialize(value));
  }

  @Override
  public Object deserializeValue(Object serializedVersion) throws IOException {
    // return JsonUtils.fromJson((String) serliazedVersion);
    return SerializationUtils.deserialize(((Blob) serializedVersion).getBytes());
  }

  @Override
  public void handleFanoutTask(FanoutTask fanoutTask) throws NoSuchObjectException {
    Key fanoutTaskRecordKey = fanoutTask.getRecordKey();
    // Fetch the fanoutTaskRecord outside of any transaction
    Entity entity = null;
    try {
      entity = dataStore.get(null, fanoutTaskRecordKey);
    } catch (EntityNotFoundException e) {
      throw new NoSuchObjectException(fanoutTaskRecordKey.toString(), e);
    }
    FanoutTaskRecord ftRecord = new FanoutTaskRecord(entity);
    byte[] encodedBytes = ftRecord.getPayload();
    taskQueue.enqueue(FanoutTask.decodeTasks(encodedBytes));
  }


  // visible for testing
  public Iterable<Entity> queryAll(String kind, Key rootJobKey, boolean keysOnly,
      FetchOptions fetchOptions) {
    Query query = new Query(kind);
    if (keysOnly) {
      query.setKeysOnly();
    }
    query.addFilter(PipelineModelObject.ROOT_JOB_KEY_PROPERTY, Query.FilterOperator.EQUAL,
        rootJobKey);
    PreparedQuery preparedQuery = dataStore.prepare(query);
    Iterable<Entity> returnValue;
    if (null != fetchOptions) {
      returnValue = preparedQuery.asIterable(fetchOptions);
    } else {
      returnValue = preparedQuery.asIterable();
    }
    return returnValue;
  }

  private interface Instantiator<E extends PipelineModelObject> {
    E newObject(Entity entity);
  }

  private <E extends PipelineModelObject> void putAll(Map<Key, E> listOfObjects,
      Instantiator<E> instantiator, String kind, Key rootJobKey) {
    for (Entity entity : queryAll(kind, rootJobKey, false, null)) {
      listOfObjects.put(entity.getKey(), instantiator.newObject(entity));
    }
  }

  @Override
  public PipelineObjects queryFullPipeline(final Key rootJobKey) {
    final Map<Key, JobRecord> jobs = new HashMap<Key, JobRecord>();
    final Map<Key, Slot> slots = new HashMap<Key, Slot>();
    final Map<Key, Barrier> barriers = new HashMap<Key, Barrier>();
    final Map<Key, JobInstanceRecord> jobInstanceRecords = new HashMap<Key, JobInstanceRecord>();
    Map<Key, ExceptionRecord> failureRecords = new HashMap<Key, ExceptionRecord>();
    putAll(barriers, new Instantiator<Barrier>() {
      @Override
      public Barrier newObject(Entity entity) {
        return new Barrier(entity);
      }
    }, Barrier.DATA_STORE_KIND, rootJobKey);
    putAll(slots, new Instantiator<Slot>() {
      @Override
      public Slot newObject(Entity entity) {
        return new Slot(entity);
      }
    }, Slot.DATA_STORE_KIND, rootJobKey);
    putAll(jobs, new Instantiator<JobRecord>() {
      @Override
      public JobRecord newObject(Entity entity) {
        JobRecord jobRecord = new JobRecord(entity);
        return jobRecord;
      }
    }, JobRecord.DATA_STORE_KIND, rootJobKey);
    putAll(jobInstanceRecords, new Instantiator<JobInstanceRecord>() {
      @Override
      public JobInstanceRecord newObject(Entity entity) {
        return new JobInstanceRecord(entity);
      }
    }, JobInstanceRecord.DATA_STORE_KIND, rootJobKey);
    putAll(failureRecords, new Instantiator<ExceptionRecord>() {
      @Override
      public ExceptionRecord newObject(Entity entity) {
        return new ExceptionRecord(entity);
      }
    }, ExceptionRecord.DATA_STORE_KIND, rootJobKey);
    return new PipelineObjects(
        rootJobKey, jobs, slots, barriers, jobInstanceRecords, failureRecords);
  }

  /**
   * Delete up to N entities of the specified kind, with the specified
   * rootJobKey
   * 
   * @return The number of entities deleted.
   */
  private int deleteN(String kind, Key rootJobKey, int n) {
    if (n < 1) {
      throw new IllegalArgumentException("n must be positive");
    }
    logger.info("Deleting  " + n + " " + kind + "s with rootJobKey=" + rootJobKey);
    List<Key> keyList = new LinkedList<Key>();
    FetchOptions fetchOptions = FetchOptions.Builder.withLimit(n).chunkSize(Math.min(n, 500));
    for (Entity entity : queryAll(kind, rootJobKey, true, fetchOptions)) {
      keyList.add(entity.getKey());
    }
    dataStore.delete(keyList);
    return keyList.size();
  }

  private void deleteAll(String kind, Key rootJobKey) {
    logger.info("Deleting all " + kind + " with rootJobKey=" + rootJobKey);
    while (deleteN(kind, rootJobKey, 2000) > 0) {
      continue;
    }
  }

  /**
   * Delete all datastore entities corresponding to the given pipeline.
   * 
   * @param rootJobKey The root job key identifying the pipeline
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
  @Override
  public void deletePipeline(Key rootJobKey, boolean force, boolean async)
      throws NoSuchObjectException, IllegalStateException {

    if (!force) {
      JobRecord rootJobRecord = queryJob(rootJobKey, JobRecord.InflationType.NONE);
      switch (rootJobRecord.getState()) {
        case FINALIZED:
        case STOPPED:
          break;
        default:
          throw new IllegalStateException("Pipeline is still running: " + rootJobRecord);
      }
    }
    if (async) {
      // We do all the checks above before bothering to enqueue a task.
      // They will have to be done again when the task is processed.
      DeletePipelineTask task = new DeletePipelineTask(rootJobKey, null, force);
      taskQueue.enqueue(task);
      return;
    }
    deleteAll(JobRecord.DATA_STORE_KIND, rootJobKey);
    deleteAll(Slot.DATA_STORE_KIND, rootJobKey);
    deleteAll(Barrier.DATA_STORE_KIND, rootJobKey);
    deleteAll(JobInstanceRecord.DATA_STORE_KIND, rootJobKey);
    deleteAll(FanoutTaskRecord.DATA_STORE_KIND, rootJobKey);
  }

}
