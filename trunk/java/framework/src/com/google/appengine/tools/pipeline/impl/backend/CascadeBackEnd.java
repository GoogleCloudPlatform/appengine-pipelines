// Copyright 2010 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.CascadeModelObject;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.io.IOException;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface CascadeBackEnd {
  public void save(UpdateSpec updateSpec);

  /**
   * {@code inflateForRun = true} means that
   * <ul>
   * <li>{@link JobRecord#getRunBarrierInflated()} will not return {@code null};
   * and
   * <li>for the returned {@link Barrier} {@link Barrier#getWaitingOnInflated()}
   * will not return {@code null}; and
   * <li>
   * <li> {@link JobRecord#getOutputSlotInflated()} will not return {@code null};
   * and {@link JobRecord#getFinalizeBarrierInflated()} will not return {@code
   * null}
   * </ul>
   * <p>
   * {@code inflateForFinalize = true} means that
   * <ul>
   * <li> {@link JobRecord#getOutputSlotInflated()} will not return {@code null};
   * and
   * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return {@code
   * null}; and
   * <li>for the returned {@link Barrier} the method
   * {@link Barrier#getWaitingOnInflated()} will not return {@code null}.
   * </ul>
   */
  public JobRecord queryJob(Key key, boolean inflateForRun, boolean inflateForFinalize)
      throws NoSuchObjectException;

  /**
   * {@code inflate = true} means that {@link Slot#getWaitingOnMeInflated()}
   * will not return {@code null} and also that for each of the {@link Barrier
   * Barriers} returned from that method {@link Barrier#getWaitingOnInflated()}
   * will not return {@code null}.
   */
  public Slot querySlot(Key key, boolean inflate) throws NoSuchObjectException;

  /**
   * Implementations of this method must guarantee the following semantics:
   * <ol>
   * <li>The new state of {@code barrier} and {@code job} will not be persisted
   * unless {@code task} is successfully enqueued
   * <li>Two threads executing this method simulataneously on the same barrier
   * will not both successfully enqueue the task.
   * </ol>
   */
  public void releaseBarrier(
      Barrier barrier, JobRecord job, JobRecord.State newJobState, Task task);

  public Object serlializeValue(Object value) throws IOException;

  public Object deserializeValue(Object serliazedVersion) throws IOException;

  public Key generateKey(CascadeModelObject newObject);

  public void handleFanoutTask(FanoutTask fanoutTask);

  public PipelineObjects queryFullPipeline(Key rootJobKey);

  public void deletePipeline(Key rootJobKey) throws NoSuchObjectException, IllegalStateException;

}
