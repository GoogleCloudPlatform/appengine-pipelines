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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineModelObject;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A specification of multiple updates to the state of the Pipeline model
 * objects that should be persisted to the data store, as well as new tasks that
 * should be enqueued.
 * <p>
 * An {@code UpdateSpec} is organized into the following sub-groups:
 * <ol>
 * <li>A {@link #getNonTransactionalGroup() non-transactional group}
 * <li>A set of {@link #getOrCreateTransaction(String) named transactional groups}
 * <li>A {@link #getFinalTransaction() final transactional group}.
 * <ol>
 *
 * When an {@code UpdateSpec} is {@link PipelineBackEnd#save saved},
 * the groups will be saved in the following order using the following
 * transactions:
 * <ol>
 * <li>Each element of the {@link #getNonTransactionalGroup() non-transactional
 * group} will be saved non-transactionally. Then,
 * <li>for each of the {@link #getOrCreateTransaction(String) named transactional
 * groups}, all of the objects in the group will be saved in a single
 * transaction. The named transactional groups will be saved in random order.
 * Finally,
 * <li>each element of the {@link #getFinalTransaction() final transactional
 * group} will be saved in a transaction.
 * <ol>
 *
 * The {@link #getFinalTransaction() final transactional group} is a
 * {@link TransactionWithTasks} and so may contain {@link Task tasks}. The tasks
 * will be enqueued in the final transaction. If there are too many tasks to
 * enqueue in a single transaction then instead a single
 * {@link com.google.appengine.tools.pipeline.impl.tasks.FanoutTask} will be
 * enqueued.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class UpdateSpec {

  private Group nonTransactionalGroup = new Group();
  private Map<String, Transaction> transactions = new HashMap<>(10);
  private TransactionWithTasks finalTransaction = new TransactionWithTasks();
  private Key rootJobKey;

  public UpdateSpec(Key rootJobKey) {
    this.rootJobKey = rootJobKey;
  }

  public void setRootJobKey(Key rootJobKey) {
    this.rootJobKey = rootJobKey;
  }

  public Key getRootJobKey() {
    return rootJobKey;
  }

  public Transaction getOrCreateTransaction(String transactionName) {
    Transaction transaction = transactions.get(transactionName);
    if (null == transaction) {
      transaction = new Transaction();
      transactions.put(transactionName, transaction);
    }
    return transaction;
  }

  public Collection<Transaction> getTransactions() {
    return transactions.values();
  }

  public TransactionWithTasks getFinalTransaction() {
    return finalTransaction;
  }

  public Group getNonTransactionalGroup() {
    return nonTransactionalGroup;
  }

  /**
   * A group of Pipeline model objects that should be saved to the data store.
   * The model object types are:
   * <ol>
   * <li> {@link Barrier}
   * <li> {@link Slot}
   * <li> {@link JobRecord}
   * <li> {@link JobInstanceRecord}
   * </ol>
   * The objects are stored in maps keyed by their {@link Key}, so there is no
   * danger of inadvertently adding the same object twice.
   *
   * @author rudominer@google.com (Mitch Rudominer)
   */
  public static class Group {
    private static final int INITIAL_SIZE = 20;

    private Map<Key, JobRecord> jobMap = new HashMap<>(INITIAL_SIZE);
    private Map<Key, Barrier> barrierMap = new HashMap<>(INITIAL_SIZE);
    private Map<Key, Slot> slotMap = new HashMap<>(INITIAL_SIZE);
    private Map<Key, JobInstanceRecord> jobInstanceMap = new HashMap<>(INITIAL_SIZE);
    private Map<Key, ExceptionRecord> failureMap = new HashMap<>(INITIAL_SIZE);

    private static <E extends PipelineModelObject> void put(Map<Key, E> map, E object) {
      map.put(object.getKey(), object);
    }

    /**
     * Include the given Barrier in the group of objects to be saved.
     */
    public void includeBarrier(Barrier barrier) {
      put(barrierMap, barrier);
    }

    public Collection<Barrier> getBarriers() {
      return barrierMap.values();
    }

    /**
     * Include the given JobRecord in the group of objects to be saved.
     */
    public void includeJob(JobRecord job) {
      put(jobMap, job);
    }

    public Collection<JobRecord> getJobs() {
      return jobMap.values();
    }

    /**
     * Include the given Slot in the group of objects to be saved.
     */
    public void includeSlot(Slot slot) {
      put(slotMap, slot);
    }

    public Collection<Slot> getSlots() {
      return slotMap.values();
    }

    /**
     * Include the given JobInstanceRecord in the group of objects to be saved.
     */
    public void includeJobInstanceRecord(JobInstanceRecord record) {
      put(jobInstanceMap, record);
    }

    public Collection<JobInstanceRecord> getJobInstanceRecords() {
      return jobInstanceMap.values();
    }

    public void includeException(ExceptionRecord failureRecord) {
      put(failureMap, failureRecord);
    }

    public Collection<ExceptionRecord> getFailureRecords() {
      return failureMap.values();
    }
  }

  /**
   * An extension of {@link Group} with the added implication that all objects
   * added to the group must be saved in a single data store transaction.
   *
   * @author rudominer@google.com (Mitch Rudominer)
   */
  public static class Transaction extends Group {
  }

  /**
   * An extension of {@link Transaction} that also accepts
   * {@link Task Tasks}. Each task included in the group will
   * be enqueued to the task queue as part of the same transaction
   * in which the objects are saved. If there are too many tasks
   * to include in a single transaction then a
   * {@link com.google.appengine.tools.pipeline.impl.tasks.FanoutTask} will be
   * used.
   *
   * @author rudominer@google.com (Mitch Rudominer)
   */
  public class TransactionWithTasks extends Transaction {
    private static final int INITIAL_SIZE = 20;
    private final Set<Task> taskSet = new HashSet<>(INITIAL_SIZE);

    public void registerTask(Task task) {
      taskSet.add(task);
    }

    /**
     * @return Unmodifiable collection of Tasks.
     */
    public Collection<Task> getTasks() {
      return Collections.unmodifiableCollection(taskSet);
    }
  }
}
