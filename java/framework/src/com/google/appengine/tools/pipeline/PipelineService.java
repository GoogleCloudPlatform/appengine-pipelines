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
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)}
   */
  String startNewPipeline(Job0<?> jobInstance, JobSetting... settings);

  /**
   * Start a new Pipeline by specifying the root job and its arguments This
   * version of the method is for root jobs that take one argument.
   * 
   * @param <T1> The type of the first argument to the root job
   * @param jobInstance A job instance to use as the root job of the Pipeline
   * @param arg1 The first argument to the root job
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)}
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
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)}
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
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)}
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
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)}
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
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)}
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
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)},
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
   *        preferable to use one of the type-safe versions of {@code
   *        startNewPipeline()} instead of using this method.
   * @param settings Optional {@code JobSettings}. These apply only to the root
   *        job
   * @return The pipeline handle. This String uniquely identifies the newly
   *         started Pipeline. It also uniquely identifies the root job of the
   *         Pipeline. The String may be used as an argument to
   *         {@link #getJobInfo(String)}, {@link #stopPipeline(String)}, and
   *         {@link #deletePipelineRecords(String)}
   */
  String startNewPipelineUnchecked(Job<?> jobInstance, Object[] arguments, JobSetting... settings);

  /**
   * Stop a Pipeline.
   * 
   * @param pipelineHandle The unique identifier returned from one of the method
   *        {@code startNewPipeline()} methods
   * @throws NoSuchObjectException If the framework cannot find a Pipeline with
   *         the given identifer.
   */
  void stopPipeline(String pipelineHandle) throws NoSuchObjectException;

  /**
   * Delete all the records associated with a Pipeline from the Datastore. This
   * method may only invoked on a Pipeline that has already completed or been
   * stopped or aborted.
   * 
   * @param pipelineHandle the unique identifer returned from one of the method
   *        {@code startNewPipeline() methods}
   * @throws NoSuchObjectException If the framework cannot find a Pipeline with
   *         the given identifer.
   * @throws IllegalStateException If the specified Pipeline is still running
   */
  void deletePipelineRecords(String pipelineHandle) throws NoSuchObjectException,
      IllegalStateException;

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
   *         given identifer.
   */
  JobInfo getJobInfo(String jobHandle) throws NoSuchObjectException;


  /**
   * This method is used to give the framework a value that was provided
   * asynchronously by some external agent and that some job is currently
   * waiting on. We call such a value a {@link PromisedValue}.
   * 
   * @param promiseHandle The unique identifer for the {@link PromisedValue}
   *        obtained from {@link PromisedValue#getHandle()}.
   * @param value The value being submitted to the framework. The type of the
   *        value must match the type of the {@link PromisedValue}.
   * @throws NoSuchObjectException If the framework cannot find a
   *         {@link PromisedValue} with the specified unique identifier.
   */
  void submitPromisedValue(String promiseHandle, Object value) throws NoSuchObjectException;
}
