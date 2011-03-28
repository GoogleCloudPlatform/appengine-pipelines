// Copyright 2010 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.backend;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.CascadeModelObject;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;
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
public class AppEngineBackEnd implements CascadeBackEnd {

  private static final Logger logger = Logger.getLogger(AppEngineBackEnd.class.getName());
  private static final int MAX_TRANSACTIONAL_TASKS = 5;

  private static Random random = new Random();

  private DatastoreService dataStore;
  private AppEngineTaskQueue taskQueue;

  {
    dataStore = DatastoreServiceFactory.getDatastoreService();
    taskQueue = new AppEngineTaskQueue();
  }

  private void putAll(Collection<? extends CascadeModelObject> objects) {
    List<Entity> entityList = new ArrayList<Entity>(objects.size());
    for (CascadeModelObject x : objects) {
      logger.finest("Storing: " + x);
      entityList.add(x.toEntity());
    }
    dataStore.put(entityList);
  }

  private void saveAll(UpdateSpec updateSpec) {
    putAll(updateSpec.getBarriers());
    putAll(updateSpec.getJobs());
    putAll(updateSpec.getSlots());
    putAll(updateSpec.getJobInstanceRecords());
    Collection<Task> tasks = updateSpec.getTasks();
    if (tasks.size() > MAX_TRANSACTIONAL_TASKS) {
      FanoutTask fannoutTask = new FanoutTask(new ArrayList<Task>(tasks));
      taskQueue.enqueue(fannoutTask);
    } else {
      for (Task task : updateSpec.getTasks()) {
        taskQueue.enqueue(task);
      }
    }
  }

  private void transactionallySaveAll(UpdateSpec updateSpec) {
    Transaction transaction = dataStore.beginTransaction();
    try {
      saveAll(updateSpec);
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
            "ConcurrentModificationException during " + operation.getName() + " attempt " + attempts
                + ".";
        if (attempts++ < 5) {
          logger.finest(logMessage + " Trying again.");
          try {
            // Sleep between 0.2 and 4.2 seconds
            Thread.sleep((long) (random.nextFloat() + 0.05) * 4000);
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

  public void save(final UpdateSpec updateSpec) {
    tryFiveTimes(new Operation("save") {
      @Override
      public void perform() {
        transactionallySaveAll(updateSpec);
      }
    });
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

  /**
   * {@inheritDoc}
   */
  public JobRecord queryJob(Key jobKey, boolean inflateForRun, boolean inflateForFinalize)
      throws NoSuchObjectException {
    try {
      Entity entity = transactionallyQueryEntity(jobKey);
      JobRecord jobRecord = new JobRecord(entity);
      Barrier runBarrier = null;
      Barrier finalizeBarrier = null;
      Slot outputSlot = null;
      JobInstanceRecord jobInstanceRecord = null;
      if (inflateForRun) {
        runBarrier = queryBarrier(jobRecord.getRunBarrierKey(), true, true);
        finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), false, true);
        jobInstanceRecord = queryJobInstanceRecord(jobRecord.getJobInstanceKey());
        outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
      }
      if (inflateForFinalize) {
        finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), true, true);
        outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
      }
      jobRecord.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord);
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
    Map<Key, Entity> entityMap = transactionallyQueryEntities(keySet);
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
   * {@inheritDoc}
   */
  public Slot querySlot(Key slotKey, boolean inflate) throws NoSuchObjectException {
    Entity entity;
    try {
      entity = transactionallyQueryEntity(slotKey);
    } catch (EntityNotFoundException e) {
      throw new NoSuchObjectException(slotKey.toString(), e);
    }
    Slot slot = new Slot(entity);
    if (inflate) {
      // Step 1. Query for all barriers that are waiting on this slot
      Key ancestor = slot.getRootJobKey();
      Query query = new Query(Barrier.DATA_STORE_KIND, ancestor);
      query.addFilter(Barrier.WAITING_ON_KEYS_PROPERTY, Query.FilterOperator.EQUAL, slotKey);
      Iterable<Entity> result = null;
      Transaction transaction = dataStore.beginTransaction();
      try {
        PreparedQuery preparedQuery = dataStore.prepare(query);
        int numExpectedBarriers = slot.getWaitingOnMeKeys().size();
        result = preparedQuery.asIterable(withLimit(numExpectedBarriers));
      } finally {
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
      // Step 2. Convert to a map from key to barrier
      Map<Key, Barrier> barrierMap = new HashMap<Key, Barrier>(20);
      for (Entity e : result) {
        Barrier barrier = new Barrier(e);
        barrierMap.put(barrier.getKey(), barrier);
      }
      // Step 3. Inflate
      slot.inflate(barrierMap);

      // Step 4. Inflate each of the barriers.
      inflateBarriers(barrierMap.values());
    }
    return slot;
  }


  /**
   * We must guarantee the following semantics:
   * <ol>
   * <li>The new state of {@code barrier} and {@code job} will not be persisted
   * unless {@code task} is successfully enqueued
   * <li>Two threads executing this method simulataneously on the same barrier
   * will not both successfully enqueue the task. The latter is achieved because
   * the task is named.
   * </ol>
   */
  public void releaseBarrier(final Barrier barrier, final JobRecord job,
      final JobRecord.State newJobState, final Task task) {
    tryFiveTimes(new Operation("release " + barrier) {
      @Override
      public void perform() {
        transactionallyReleaseBarrier(barrier, job, newJobState, task);
      }
    });
  }

  private void transactionallyReleaseBarrier(
      Barrier barrier, JobRecord job, JobRecord.State newJobState, Task task) {
    job.setState(newJobState);
    UpdateSpec updateSpec = new UpdateSpec();
    updateSpec.includeJob(job);
    updateSpec.registerTask(task);
    Transaction transaction = dataStore.beginTransaction();
    try {
      try {
        // Query the barrier within the current transaction
        boolean startNewTransaction = false;
        barrier = queryBarrier(barrier.getKey(), false, startNewTransaction);
      } catch (EntityNotFoundException e) {
        throw new RuntimeException(e);
      }
      if (barrier.isReleased()) {
        logger.finest("Already released: " + barrier);
      } else {
        barrier.setReleased();
        updateSpec.includeBarrier(barrier);
        saveAll(updateSpec);
        transaction.commit();
        logger.finest("Committed release: " + barrier);
      }
    } finally {
      if (transaction.isActive()) {
        transaction.rollback();
      }
    }
  }

  public Object serlializeValue(Object value) throws IOException {
    // return JsonUtils.toJson(value);
    return new Blob(SerializationUtils.serialize(value));
  }

  public Object deserializeValue(Object serializedVersion) throws IOException {
    // return JsonUtils.fromJson((String) serliazedVersion);
    return SerializationUtils.deserialize(((Blob) serializedVersion).getBytes());
  }

  public Key generateKey(CascadeModelObject newObject) {
    Key rootJobKey = newObject.getRootJobKey();
    String name = GUIDGenerator.nextGUID();
    String kind = newObject.getDatastoreKind();
    Key key;
    if (null == rootJobKey) {
      if (JobRecord.DATA_STORE_KIND.equals(kind)) {
        // This object is the root job.
        key = KeyFactory.createKey(kind, name);
      } else {
        throw new RuntimeException("rootJobKeyString is null for " + newObject);
      }
    } else {
      key = rootJobKey.getChild(kind, name);
    }
    return key;
  }

  public void handleFanoutTask(FanoutTask fanoutTask) {
    UpdateSpec updateSpec = new UpdateSpec();
    Collection<Task> taskCollection = fanoutTask.getTasks();
    for (Task task : taskCollection) {
      taskQueue.enqueue(task);
    }
  }
  
  
  
  private Iterable<Entity> queryAll(String kind, Key rootJobKey, boolean keysOnly) {
    Query query = new Query(kind);
    if (keysOnly){
      query.setKeysOnly();
    }
    query.addFilter(
        CascadeModelObject.ROOT_JOB_KEY_PROPERTY, Query.FilterOperator.EQUAL, rootJobKey);
    PreparedQuery preparedQuery = dataStore.prepare(query);
    return preparedQuery.asIterable();
  }

  private interface Instantiator<E extends CascadeModelObject> {
    E newObject(Entity entity);
  }

  private <E extends CascadeModelObject> void putAll(
      Map<Key, E> listOfObjects, Instantiator<E> instantiator, String kind, Key rootJobKey) {
    for (Entity entity : queryAll(kind, rootJobKey, false)) {
      listOfObjects.put(entity.getKey(), instantiator.newObject(entity));
    }
  }


  public PipelineObjects queryFullPipeline(final Key rootJobKey) {
    final Map<Key, JobRecord> jobs = new HashMap<Key, JobRecord>();
    final Map<Key, Slot> slots = new HashMap<Key, Slot>();
    final Map<Key, Barrier> barriers = new HashMap<Key, Barrier>();
    final Map<Key, JobInstanceRecord> jobInstanceRecords = new HashMap<Key, JobInstanceRecord>();
    putAll(barriers, new Instantiator<Barrier>() {
      public Barrier newObject(Entity entity) {
        return new Barrier(entity);
      }
    }, Barrier.DATA_STORE_KIND, rootJobKey);
    putAll(slots, new Instantiator<Slot>() {
      public Slot newObject(Entity entity) {
        return new Slot(entity);
      }
    }, Slot.DATA_STORE_KIND,rootJobKey);
    putAll(jobs, new Instantiator<JobRecord>() {
      public JobRecord newObject(Entity entity) {
        JobRecord jobRecord = new JobRecord(entity);
        return jobRecord;
      }
    }, JobRecord.DATA_STORE_KIND,rootJobKey);
    putAll(jobInstanceRecords, new Instantiator<JobInstanceRecord>() {
      public JobInstanceRecord newObject(Entity entity) {
        return new JobInstanceRecord(entity);
      }
    }, JobInstanceRecord.DATA_STORE_KIND,rootJobKey);
    return new PipelineObjects(rootJobKey, jobs, slots, barriers, jobInstanceRecords);
  }
  
  private void deleteAll(String kind, Key rootJobKey){
    List<Key> keyList = new LinkedList<Key>();
    for (Entity entity : queryAll(kind, rootJobKey, true)){
      keyList.add(entity.getKey());
    }
    dataStore.delete(keyList);
  }
  
  public void deletePipeline(Key rootJobKey) throws NoSuchObjectException, IllegalStateException {
    JobRecord rootJobRecord = queryJob(rootJobKey, false, false);
    switch(rootJobRecord.getState()){
      case WAITING_FOR_RUN_SLOTS:
      case READY_TO_RUN:
      case WAITING_FOR_FINALIZE_SLOT:
      case READY_TO_FINALIZE:
      case RETRY:
        throw new IllegalStateException("Pipeline is still running: " + rootJobRecord);
      case FINALIZED:
      case STOPPED:
        // OK
    }
    deleteAll(JobRecord.DATA_STORE_KIND, rootJobKey);
    deleteAll(Slot.DATA_STORE_KIND, rootJobKey);
    deleteAll(Barrier.DATA_STORE_KIND, rootJobKey);
    deleteAll(JobInstanceRecord.DATA_STORE_KIND, rootJobKey);
  }

}
