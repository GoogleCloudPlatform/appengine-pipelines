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

import static com.google.appengine.api.datastore.Query.FilterOperator.EQUAL;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN;
import static com.google.appengine.tools.pipeline.impl.model.JobRecord.ROOT_JOB_DISPLAY_NAME;
import static com.google.appengine.tools.pipeline.impl.model.PipelineModelObject.ROOT_JOB_KEY_PROPERTY;
import static com.google.appengine.tools.pipeline.impl.util.TestUtils.throwHereForTesting;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.PropertyProjection;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.NonRetriableException;
import com.google.appengine.tools.cloudstorage.RetriesExhaustedException;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.FanoutTaskRecord;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineModelObject;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.ShardedValue;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.appengine.tools.pipeline.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class AppEngineBackEnd implements PipelineBackEnd {

  private static final RetryParams RETRY_PARAMS = new RetryParams.Builder()
      .retryDelayBackoffFactor(2)
      .initialRetryDelayMillis(300)
      .maxRetryDelayMillis(5000)
      .retryMinAttempts(5)
      .retryMaxAttempts(5)
      .build();

  private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder().retryOn(
      ConcurrentModificationException.class, DatastoreTimeoutException.class,
      DatastoreFailureException.class)
      .abortOn(EntityNotFoundException.class, NoSuchObjectException.class).build();

  private static final Logger logger = Logger.getLogger(AppEngineBackEnd.class.getName());

  private static final int MAX_BLOB_BYTE_SIZE = 1000000;

  private final DatastoreService dataStore;
  private final AppEngineTaskQueue taskQueue;

  public AppEngineBackEnd() {
    dataStore = DatastoreServiceFactory.getDatastoreService();
    taskQueue = new AppEngineTaskQueue();
  }

  private void putAll(Collection<? extends PipelineModelObject> objects) {
    if (objects.isEmpty()) {
      return;
    }
    List<Entity> entityList = new ArrayList<>(objects.size());
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

  private boolean transactionallySaveAll(UpdateSpec.Transaction transactionSpec,
      QueueSettings queueSettings, Key rootJobKey, Key jobKey, JobRecord.State... expectedStates) {
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
          logger.info("Job " + jobRecord + " is not in one of the expected states: "
              + Arrays.asList(expectedStates)
              + " and so transactionallySaveAll() will not continue.");
          return false;
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
          FanoutTask fannoutTask = new FanoutTask(ftRecord.getKey(), queueSettings);
          taskQueue.enqueue(fannoutTask);
        }
      }
      transaction.commit();
    } finally {
      if (transaction.isActive()) {
        transaction.rollback();
      }
    }
    return true;
  }

  private abstract class Operation<R> implements Callable<R> {

    private final String name;

    Operation(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private <R> R tryFiveTimes(final Operation<R> operation) {
    try {
      return RetryHelper.runWithRetries(operation, RETRY_PARAMS, EXCEPTION_HANDLER);
    } catch (RetriesExhaustedException|NonRetriableException e) {
      if (e.getCause() instanceof RuntimeException) {
        logger.info(e.getCause().getMessage() + " during " + operation.getName()
            + " throwing after multiple attempts ");
        throw (RuntimeException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  @Override
  public void enqueue(Task task) {
    taskQueue.enqueue(task);
  }

  @Override
  public boolean saveWithJobStateCheck(final UpdateSpec updateSpec,
      final QueueSettings queueSettings, final Key jobKey,
      final JobRecord.State... expectedStates) {
    tryFiveTimes(new Operation<Void>("save") {
      @Override
      public Void call() {
        saveAll(updateSpec.getNonTransactionalGroup());
        return null;
      }
    });
    for (final UpdateSpec.Transaction transactionSpec : updateSpec.getTransactions()) {
      tryFiveTimes(new Operation<Void>("save") {
        @Override
        public Void call() {
          transactionallySaveAll(transactionSpec, queueSettings, updateSpec.getRootJobKey(), null);
          return null;
        }
      });
    }

    // TODO(user): Replace this with plug-able hooks that could be used by tests,
    // if needed could be restricted to package-scoped tests.
    // If a unit test requests us to do so, fail here.
    throwHereForTesting("AppEngineBackeEnd.saveWithJobStateCheck.beforeFinalTransaction");
    final AtomicBoolean wasSaved = new AtomicBoolean(true);
    tryFiveTimes(new Operation<Void>("save") {
      @Override
      public Void call() {
        wasSaved.set(transactionallySaveAll(updateSpec.getFinalTransaction(), queueSettings,
            updateSpec.getRootJobKey(), jobKey, expectedStates));
        return null;
      }
    });
    return wasSaved.get();
  }

  @Override
  public void save(UpdateSpec updateSpec, QueueSettings queueSettings) {
    saveWithJobStateCheck(updateSpec, queueSettings, null);
  }

  @Override
  public JobRecord queryJob(final Key jobKey, final JobRecord.InflationType inflationType)
      throws NoSuchObjectException {
    Entity entity = getEntity("queryJob", jobKey);
    JobRecord jobRecord = new JobRecord(entity);
    Barrier runBarrier = null;
    Barrier finalizeBarrier = null;
    Slot outputSlot = null;
    JobInstanceRecord jobInstanceRecord = null;
    ExceptionRecord failureRecord = null;
    switch (inflationType) {
      case FOR_RUN:
        runBarrier = queryBarrier(jobRecord.getRunBarrierKey(), true);
        finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), false);
        jobInstanceRecord =
            new JobInstanceRecord(getEntity("queryJob", jobRecord.getJobInstanceKey()));
        outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
        break;
      case FOR_FINALIZE:
        finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), true);
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
  }

  /**
   * {@code inflate = true} means that {@link Barrier#getWaitingOnInflated()}
   * will not return {@code null}.
   */
  private Barrier queryBarrier(Key barrierKey, boolean inflate) throws NoSuchObjectException {
    Entity entity = getEntity("queryBarrier", barrierKey);
    Barrier barrier = new Barrier(entity);
    if (inflate) {
      Collection<Barrier> barriers = new ArrayList<>(1);
      barriers.add(barrier);
      inflateBarriers(barriers);
    }
    logger.finest("Querying returned: " + barrier);
    return barrier;
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
    Set<Key> keySet = new HashSet<>(barriers.size() * 5);
    for (Barrier barrier : barriers) {
      for (Key key : barrier.getWaitingOnKeys()) {
        keySet.add(key);
      }
    }
    // Step 2. Query the datastore for the Slot entities
    Map<Key, Entity> entityMap = getEntities("inflateBarriers", keySet);

    // Step 3. Convert into map from key to Slot
    Map<Key, Slot> slotMap = new HashMap<>(entityMap.size());
    for (Key key : keySet) {
      Slot s = new Slot(entityMap.get(key));
      slotMap.put(key, s);
    }
    // Step 4. Inflate each of the barriers
    for (Barrier barrier : barriers) {
      barrier.inflate(slotMap);
    }
  }

  @Override
  public Slot querySlot(Key slotKey, boolean inflate) throws NoSuchObjectException {
    Entity entity = getEntity("querySlot", slotKey);
    Slot slot = new Slot(entity);
    if (inflate) {
      Map<Key, Entity> entities = getEntities("querySlot", slot.getWaitingOnMeKeys());
      Map<Key, Barrier> barriers = new HashMap<>(entities.size());
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
    Entity entity = getEntity("ReadExceptionRecord", failureKey);
    return new ExceptionRecord(entity);
  }

  @Override
  public Object serializeValue(PipelineModelObject model, Object value) throws IOException {
    byte[] bytes = SerializationUtils.serialize(value);
    if (bytes.length < MAX_BLOB_BYTE_SIZE) {
      return new Blob(bytes);
    }
    int shardId = 0;
    int offset = 0;
    final List<Entity> shardedValues = new ArrayList<>(bytes.length / MAX_BLOB_BYTE_SIZE + 1);
    while (offset < bytes.length) {
      int limit = offset + MAX_BLOB_BYTE_SIZE;
      byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.min(limit, bytes.length));
      offset = limit;
      shardedValues.add(new ShardedValue(model, shardId++, chunk).toEntity());
    }
    return tryFiveTimes(new Operation<List<Key>>("serializeValue") {
      @Override
      public List<Key> call() {
        Transaction tx = dataStore.beginTransaction();
        List<Key> keys;
        try {
          keys = dataStore.put(tx, shardedValues);
          tx.commit();
        } finally {
          if (tx.isActive()) {
            tx.rollback();
          }
        }
        return keys;
      }
    });
  }

  @Override
  public Object deserializeValue(PipelineModelObject model, Object serializedVersion)
      throws IOException {
    if (serializedVersion instanceof Blob) {
      return SerializationUtils.deserialize(((Blob) serializedVersion).getBytes());
    } else {
      @SuppressWarnings("unchecked")
      Collection<Key> keys = (Collection<Key>) serializedVersion;
      Map<Key, Entity> entities = getEntities("deserializeValue", keys);
      ShardedValue[] shardedValues = new ShardedValue[entities.size()];
      int totalSize = 0;
      int index = 0;
      for (Key key : keys) {
        Entity entity = entities.get(key);
        ShardedValue shardedValue = new ShardedValue(entity);
        shardedValues[index++] = shardedValue;
        totalSize += shardedValue.getValue().length;
      }
      byte[] totalBytes = new byte[totalSize];
      int offset = 0;
      for (ShardedValue shardedValue : shardedValues) {
        byte[] shardBytes = shardedValue.getValue();
        System.arraycopy(shardBytes, 0, totalBytes, offset, shardBytes.length);
        offset += shardBytes.length;
      }
      return SerializationUtils.deserialize(totalBytes);
    }
  }

  private Map<Key, Entity> getEntities(String logString, final Collection<Key> keys) {
    Map<Key, Entity> result = tryFiveTimes(new Operation<Map<Key, Entity>>(logString) {
      @Override
      public Map<Key, Entity> call() {
        return dataStore.get(null, keys);
      }
    });
    if (keys.size() != result.size()) {
      List<Key> missing = new ArrayList<>(keys);
      missing.removeAll(result.keySet());
      throw new RuntimeException("Missing entities for keys: " + missing);
    }
    return result;
  }

  private Entity getEntity(String logString, final Key key) throws NoSuchObjectException {
    try {
      return tryFiveTimes(new Operation<Entity>(logString) {
        @Override
        public Entity call() throws EntityNotFoundException  {
            return dataStore.get(null, key);
        }
      });
    } catch (NonRetriableException|RetriesExhaustedException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EntityNotFoundException) {
        throw new NoSuchObjectException(key.toString(), cause);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void handleFanoutTask(FanoutTask fanoutTask) throws NoSuchObjectException {
    Key fanoutTaskRecordKey = fanoutTask.getRecordKey();
    // Fetch the fanoutTaskRecord outside of any transaction
    Entity entity = getEntity("handleFanoutTask", fanoutTaskRecordKey);
    FanoutTaskRecord ftRecord = new FanoutTaskRecord(entity);
    byte[] encodedBytes = ftRecord.getPayload();
    taskQueue.enqueue(FanoutTask.decodeTasks(encodedBytes));
  }

  public List<Entity> queryAll(final String kind, final Key rootJobKey) {
    Query query = new Query(kind);
    query.setFilter(new FilterPredicate(ROOT_JOB_KEY_PROPERTY, EQUAL, rootJobKey));
    final PreparedQuery preparedQuery = dataStore.prepare(query);
    final FetchOptions options = FetchOptions.Builder.withChunkSize(500);
    return tryFiveTimes(new Operation<List<Entity>>("queryFullPipeline") {
      @Override
      public List<Entity> call() {
        return preparedQuery.asList(options);
      }
    });
  }

  @Override
  public Pair<? extends Iterable<JobRecord>, String> queryRootPipelines(String classFilter,
      String cursor, final int limit) {
    Query query = new Query(JobRecord.DATA_STORE_KIND);
    Filter filter = classFilter == null || classFilter.isEmpty() ? new FilterPredicate(
        ROOT_JOB_DISPLAY_NAME, GREATER_THAN, null)
        : new FilterPredicate(ROOT_JOB_DISPLAY_NAME, EQUAL, classFilter);
    query.setFilter(filter);
    final PreparedQuery preparedQuery = dataStore.prepare(query);
    final FetchOptions fetchOptions = FetchOptions.Builder.withDefaults();
    if (limit > 0) {
      fetchOptions.limit(limit + 1);
    }
    if (cursor != null) {
      fetchOptions.startCursor(Cursor.fromWebSafeString(cursor));
    }
    return tryFiveTimes(
        new Operation<Pair<? extends Iterable<JobRecord>, String>>("queryRootPipelines") {
          @Override
          public Pair<? extends Iterable<JobRecord>, String> call() {
            QueryResultIterator<Entity> entities =
                preparedQuery.asQueryResultIterable(fetchOptions).iterator();
            Cursor dsCursor = null;
            List<JobRecord> roots = new LinkedList<>();
            while (entities.hasNext()) {
              if (limit > 0 && roots.size() >= limit) {
                dsCursor = entities.getCursor();
                break;
              }
              JobRecord jobRecord = new JobRecord(entities.next());
              roots.add(jobRecord);
            }
            return Pair.of(roots, dsCursor == null ? null : dsCursor.toWebSafeString());
          }
        });
  }

  @Override
  public Set<String> getRootPipelinesDisplayName() {
    Query query = new Query(JobRecord.DATA_STORE_KIND);
    query.addProjection(
        new PropertyProjection(JobRecord.ROOT_JOB_DISPLAY_NAME, String.class));
    query.setDistinct(true);
    final PreparedQuery preparedQuery = dataStore.prepare(query);
    return tryFiveTimes(new Operation<Set<String>>("getRootPipelinesDisplayName") {
      @Override
      public Set<String> call() {
        Set<String> pipelines = new LinkedHashSet<>();
        for (Entity entity : preparedQuery.asIterable()) {
          pipelines.add((String) entity.getProperty(JobRecord.ROOT_JOB_DISPLAY_NAME));
        }
        return pipelines;
      }
    });
  }

  @Override
  public PipelineObjects queryFullPipeline(final Key rootJobKey) {
    final Map<Key, JobRecord> jobs = new HashMap<>();
    final Map<Key, Slot> slots = new HashMap<>();
    final Map<Key, Barrier> barriers = new HashMap<>();
    final Map<Key, JobInstanceRecord> jobInstanceRecords = new HashMap<>();
    final Map<Key, ExceptionRecord> failureRecords = new HashMap<>();

    for (Entity entity : queryAll(Barrier.DATA_STORE_KIND, rootJobKey)) {
      barriers.put(entity.getKey(), new Barrier(entity));
    }
    for (Entity entity : queryAll(Slot.DATA_STORE_KIND, rootJobKey)) {
      slots.put(entity.getKey(), new Slot(entity, true));
    }
    for (Entity entity : queryAll(JobRecord.DATA_STORE_KIND, rootJobKey)) {
      jobs.put(entity.getKey(), new JobRecord(entity));
    }
    for (Entity entity : queryAll(JobInstanceRecord.DATA_STORE_KIND, rootJobKey)) {
      jobInstanceRecords.put(entity.getKey(), new JobInstanceRecord(entity));
    }
    for (Entity entity : queryAll(ExceptionRecord.DATA_STORE_KIND, rootJobKey)) {
      failureRecords.put(entity.getKey(), new ExceptionRecord(entity));
    }
    return new PipelineObjects(
        rootJobKey, jobs, slots, barriers, jobInstanceRecords, failureRecords);
  }

  private void deleteAll(final String kind, final Key rootJobKey) {
    logger.info("Deleting all " + kind + " with rootJobKey=" + rootJobKey);
    final int chunkSize = 100;
    final FetchOptions fetchOptions = FetchOptions.Builder.withChunkSize(chunkSize);
    final PreparedQuery preparedQuery = dataStore.prepare(new Query(kind).setKeysOnly().setFilter(
        new FilterPredicate(ROOT_JOB_KEY_PROPERTY, EQUAL, rootJobKey)));
    tryFiveTimes(new Operation<Void>("delete") {
      @Override
      public Void call() {
        Iterator<Entity> iter = preparedQuery.asIterator(fetchOptions);
        while (iter.hasNext()) {
          ArrayList<Key> keys = new ArrayList<>(chunkSize);
          for (int i = 0; i < chunkSize && iter.hasNext(); i++) {
            keys.add(iter.next().getKey());
          }
          logger.info("Deleting  " + keys.size() + " " + kind + "s with rootJobKey=" + rootJobKey);
          dataStore.delete(null, keys);
        }
        return null;
      }
    });
  }

  /**
   * Delete all datastore entities corresponding to the given pipeline.
   *
   * @param rootJobKey The root job key identifying the pipeline
   * @param force If this parameter is not {@code true} then this method will
   *        throw an {@link IllegalStateException} if the specified pipeline is not in the
   *        {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED} or
   *        {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED} state.
   * @param async If this parameter is {@code true} then instead of performing
   *        the delete operation synchronously, this method will enqueue a task
   *        to perform the operation.
   * @throws IllegalStateException If {@code force = false} and the specified
   *         pipeline is not in the
   *         {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED} or
   *         {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED} state.
   */
  @Override
  public void deletePipeline(Key rootJobKey, boolean force, boolean async)
      throws IllegalStateException {
    if (!force) {
      try {
        JobRecord rootJobRecord = queryJob(rootJobKey, JobRecord.InflationType.NONE);
        switch (rootJobRecord.getState()) {
          case FINALIZED:
          case STOPPED:
            break;
          default:
            throw new IllegalStateException("Pipeline is still running: " + rootJobRecord);
        }
      } catch (NoSuchObjectException ex) {
        // Consider missing rootJobRecord as a non-active job and allow further delete
      }
    }
    if (async) {
      // We do all the checks above before bothering to enqueue a task.
      // They will have to be done again when the task is processed.
      DeletePipelineTask task = new DeletePipelineTask(rootJobKey, force, new QueueSettings());
      taskQueue.enqueue(task);
      return;
    }
    deleteAll(JobRecord.DATA_STORE_KIND, rootJobKey);
    deleteAll(Slot.DATA_STORE_KIND, rootJobKey);
    deleteAll(ShardedValue.DATA_STORE_KIND, rootJobKey);
    deleteAll(Barrier.DATA_STORE_KIND, rootJobKey);
    deleteAll(JobInstanceRecord.DATA_STORE_KIND, rootJobKey);
    deleteAll(FanoutTaskRecord.DATA_STORE_KIND, rootJobKey);
  }
}
