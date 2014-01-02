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

package com.google.appengine.tools.pipeline;

/**
 * A service used to start and stop Pipeline jobs and query their status.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface PipelineService {

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take zero arguments.
   *
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)}
   */
  String startNewPipeline(Job0<?> jobInstance, JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take one argument.
   *
   * @param <T1> The type of the first argument to the root job
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arg1 The first argument to the root job
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)}
   */
  <T1> String startNewPipeline(Job1<?, T1> jobInstance, T1 arg1, JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take two arguments.
   *
   * @param <T1> The type of the first argument to the root job
   * @param <T2> The type of the second argument to the root job
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arg1 The first argument to the root job
   * @param arg2 The second argument to the root job
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)}
   */
  <T1, T2> String startNewPipeline(Job2<?, T1, T2> jobInstance, T1 arg1, T2 arg2,
      JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take three arguments.
   *
   * @param <T1> The type of the first argument to the root job
   * @param <T2> The type of the second argument to the root job
   * @param <T3> The type of the third argument to the root job
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arg1 The first argument to the root job
   * @param arg2 The second argument to the root job
   * @param arg3 The third argument to the root job
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)}
   */
  <T1, T2, T3> String startNewPipeline(Job3<?, T1, T2, T3> jobInstance, T1 arg1, T2 arg2, T3 arg3,
      JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take four arguments.
   *
   * @param <T1> The type of the first argument to the root job
   * @param <T2> The type of the second argument to the root job
   * @param <T3> The type of the third argument to the root job
   * @param <T4> The type of the fourth argument to the root job
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arg1 The first argument to the root job
   * @param arg2 The second argument to the root job
   * @param arg3 The third argument to the root job
   * @param arg4 The fourth argument to the root job
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)}
   */
  <T1, T2, T3, T4> String startNewPipeline(Job4<?, T1, T2, T3, T4> jobInstance, T1 arg1, T2 arg2,
      T3 arg3, T4 arg4, JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take five arguments.
   *
   * @param <T1> The type of the first argument to the root job
   * @param <T2> The type of the second argument to the root job
   * @param <T3> The type of the third argument to the root job
   * @param <T4> The type of the fourth argument to the root job
   * @param <T5> The type of the fifth argument to the root job
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arg1 The first argument to the root job
   * @param arg2 The second argument to the root job
   * @param arg3 The third argument to the root job
   * @param arg4 The fourth argument to the root job
   * @param arg5 The fifth argument to the root job
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)}
   */
  <T1, T2, T3, T4, T5> String startNewPipeline(Job5<?, T1, T2, T3, T4, T5> jobInstance, T1 arg1,
      T2 arg2, T3 arg3, T4 arg4, T5 arg5, JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take six arguments.
   *
   * @param <T1> The type of the first argument to the root job
   * @param <T2> The type of the second argument to the root job
   * @param <T3> The type of the third argument to the root job
   * @param <T4> The type of the fourth argument to the root job
   * @param <T5> The type of the fifth argument to the root job
   * @param <T6> The type of the sixth argument to the root job
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arg1 The first argument to the root job
   * @param arg2 The second argument to the root job
   * @param arg3 The third argument to the root job
   * @param arg4 The fourth argument to the root job
   * @param arg5 The fifth argument to the root job
   * @param arg6 The sixth argument to the root job
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)},
   */
  <T1, T2, T3, T4, T5, T6> String startNewPipeline(Job6<?, T1, T2, T3, T4, T5, T6> jobInstance,
      T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take more arguments than are
   * allowed in any of the type-safe versions of {@code startNewPipeline()}.
   *
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arguments An array of Objects to be used as the arguments of the
   *        root job. The type and number of the arguments must match the
   *        arguments of the {@code run()} method of the job. If possible, it is
   *        preferable to use one of the type-safe versions of
   *        {@code startNewPipeline()} instead of using this method.
   * @param settings Optional one or more {@code JobSetting}
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String, boolean, boolean)}
   */
  String startNewPipelineUnchecked(Job<?> jobInstance, Object[] arguments, JobSetting... settings);

  /**
   * Stops pipeline execution without giving its code any chance to perform
   * cleanup. Use {@link #cancelPipeline(String)} to request controlled pipeline
   * termination.
   *
   * @param pipelineHandle The unique identifier returned from one of the
   *        {@code startNewPipeline()} methods
   * @throws NoSuchObjectException If the framework cannot find a Pipeline with
   *         the given identifier.
   */
  void stopPipeline(String pipelineHandle) throws NoSuchObjectException;

  /**
   * Cancel all pipeline jobs. If a cancelled job has
   * {@code handleException(...)} defined it is called with
   * {@link java.util.concurrent.CancellationException} giving it a chance to
   * perform necessary cleanup.
   *
   * @param pipelineHandle The unique identifier returned from one of the
   *        {@code startNewPipeline()} methods
   * @throws NoSuchObjectException If the framework cannot find a Pipeline with
   *         the given identifier.
   */
  void cancelPipeline(String pipelineHandle) throws NoSuchObjectException;

  /**
   * Delete all the records associated with a pipeline from the Datastore.
   *
   * @param pipelineHandle The handle of the pipeline to be deleted. The
   *        specified pipeline must exist and it must not be currently running.
   * @throws NoSuchObjectException If the framework cannot find a Pipeline with
   *         the given identifier.
   * @throws IllegalStateException If the specified Pipeline is still running
   */
  void deletePipelineRecords(String pipelineHandle) throws NoSuchObjectException,
      IllegalStateException;

  /**
   * Delete all the records associated with a pipeline from the datastore.
   *
   * @param pipelineHandle The handle of the pipeline to be deleted
   * @param force If this parameter is not {@code true} then this method will
   *        throw an {@link IllegalStateException} if the specified pipeline is
   *        has not already completed or been stopped or aborted. Invoking this
   *        method with {@code force = true} should only be done in unusual
   *        circumstances. There may still be tasks on the task queue
   *        corresponding to a non-completed pipeline. These tasks must be
   *        manually deleted through the App Engine admin console.
   * @param async If this parameter is {@code true} then instead of performing
   *        the delete operation synchronously, this method will enqueue a task
   *        to perform the operation.
   * @throws NoSuchObjectException If {@code force = false} and the framework
   *         cannot find a pipeline with the given identifier. Set {@code force
   *         = true} in order to clean up a corrupt datastore.
   * @throws IllegalStateException If {@code force = false} and the specified
   *         Pipeline is still running
   */
  void deletePipelineRecords(String pipelineHandle, boolean force, boolean async)
      throws NoSuchObjectException, IllegalStateException;

  /**
   * Retrieves information about a Pipeline job. Usually this method will be
   * invoked on the handle of the root job of a Pipeline obtained from one of
   * the {@link #startNewPipeline(Job0, JobSetting...)} family of methods. But
   * it is also possible to query for the {@code JobInfo} of a non-root job by
   * passing in the String returned from
   * {@link FutureValue#getSourceJobHandle()}.
   *
   * @param jobHandle The unique identifier of a job
   * @return A {@link JobInfo} representing the specified job
   * @throws NoSuchObjectException If the framework cannot find a job with the
   *         given identifier.
   */
  JobInfo getJobInfo(String jobHandle) throws NoSuchObjectException;


  /**
   * This method is used to give the framework a value that is provided
   * asynchronously by some external agent and that some job is currently
   * waiting on. We call such a value a {@link PromisedValue}.
   *
   * @param promiseHandle The unique identifier for the {@link PromisedValue}
   *        obtained during the execution of some job via the method
   *        {@link PromisedValue#getHandle()}.
   * @param value The value being submitted to the framework. The type of the
   *        value must match the type of the {@link PromisedValue}.
   * @throws NoSuchObjectException If the framework cannot find a
   *         {@link PromisedValue} identified by {@code promiseHandel}. This
   *         exception may indicate that this method is being invoked too soon.
   *         The thread that created the {@link PromisedValue} referenced by
   *         {@code promiseHandle} may not have finished running the associated
   *         job and saving the {@link PromisedValue} yet and this would explain
   *         why the framework does not yet have an object with the given
   *         handle. For this reason, this exception should be caught by the
   *         caller and this method should be attempted again after waiting some
   *         time. If the caller has good reason to believe that he has waited
   *         long enough for the other thread to have completed its work, then
   *         this method may indicate that the Pipeline has somehow become
   *         corrupted and the caller should give up.
   * @throws OrphanedObjectException If the {@link PromisedValue} reference by
   *         {@code promiseHandle} has been orphaned during some failed
   *         execution of a job. In this case the caller should not attempt to
   *         use {@code promiseHandle} again. Most likely a different successful
   *         execution of the same job generated a different promise handle, and
   *         some other thread will be submitting the promised value via that
   *         non-orphaned handle.
   */
  void submitPromisedValue(String promiseHandle, Object value) throws NoSuchObjectException,
      OrphanedObjectException;
}
