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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.impl.FutureValueImpl;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.PromisedValueImpl;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;

import java.io.Serializable;
import java.util.List;

/**
 * The abstract ancestor class of all user job classes. A <b>job</b> is by
 * definition a subclass of this class that implements a method named <code>run
 * </code>.
 * <p>
 * In order to fully take advantage of the framework's compile-time
 * type-checking, a user's job class should subclass one of {@link Job0},
 * {@link Job1}, {@link Job2}, etc instead of directly subclassing this class,
 * and then implement the appropriate type-safe version of {@code run()}. The
 * only reason a user-written job class should directly subclass this class is
 * if the {@code run} method of the job needs to take more arguments than the
 * greatest {@code n} such that the framework offers a {@code Jobn} class.
 * <p>
 * This class contains several protected methods that may be invoked from with
 * the {@code run()} method.
 * <ul>
 * <li><b>{@code futureCall()}</b> There is a family of methods named
 * {@code futureCall()} used to specify a job node in a generated child job
 * graph. There are several type-safe versions of {@code futureCall()} such as
 * {@link #futureCall(Job0, JobSetting...)} and
 * {@link #futureCall(Job1, Value, JobSetting...)}. There is also one
 * non-type-safe version
 * {@link #futureCallUnchecked(JobSetting[], Job, Object...)} used for child
 * jobs that directly subclass {@code Job} rather than subclassing {@code Jobn}
 * for some {@code n}.
 * <li> {@link #newPromise(Class)} is used to construct a new
 * {@link PromisedValue}.
 * <li>Several methods that are <em>syntactic sugar</em>. These are methods that
 * allow one to construct objects that may be used as arguments to
 * {@code futureCall()} using less typing. These include
 * <ol>
 * <li> {@link #futureList(List)}. Constructs a new {@link FutureList}
 * <li> {@link #immediate(Object)}. Constructs a new {@link ImmediateValue}
 * <li> {@link #waitFor(FutureValue)}. Constructs a new
 * {@link JobSetting.WaitForSetting}.
 * <li> {@link #backOffFactor(int)}. Constructs a new
 * {@link JobSetting.BackoffFactor}.
 * <li> {@link #backOffSeconds(int)}. Constructs a new
 * {@link JobSetting.BackoffSeconds}.
 * <li> {@link #maxAttempts(int)}. Constructs a new
 * {@link JobSetting.MaxAttempts}.
 * </ol>
 * </ul>
 * As an example of the use of the syntactic sugar, from within a {@code run}
 * method one might write
 *
 * <pre>
 * <code>
 * FutureValue&lt;Integer&gt; x = futureCall(new MyJob(), immediate(4));
 * FutureValue&lt;String&gt; s = futureCall(new MySecondJob(), x, immediate("hello");
 * futureCall(new MyThirdJob(), waitFor(s), waitFor(x), maxAttempts(5), backoffSeconds(5));
 * </code>
 * </pre>
 * <p>
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 * @param <E> The return type of the job.
 */
public abstract class Job<E> implements Serializable {

  private static final long serialVersionUID = 868736209042268959L;

  private transient JobRecord thisJobRecord;
  private transient UpdateSpec updateSpec;
  private transient String currentRunGUID;

  // This method will be invoked by reflection from PipelineManager
  @SuppressWarnings("unused")
  private void setJobRecord(JobRecord jobRecord) {
    this.thisJobRecord = jobRecord;
  }

  // This method will be invoked by reflection from PipelineManager
  @SuppressWarnings("unused")
  private void setUpdateSpec(UpdateSpec spec) {
    this.updateSpec = spec;
  }

  // This method will be invoked by reflection from PipelineManager
  @SuppressWarnings("unused")
  private void setCurrentRunGuid(String guid) {
    this.currentRunGUID = guid;
  }

  /**
   * This is the non-type-safe version of the {@code futureCall()} family of
   * methods. Normally a user will not need to invoke this method directly.
   * Instead, one of the type-safe methods such as
   * {@link #futureCall(Job2, Value, Value, JobSetting...)} should be used. The
   * only reason a user should invoke this method directly is if
   * {@code jobInstance} is a direct subclass of {@code Job} instead of being a
   * subclass of one of the {@code Jobn} classes and the {@code run()} method of
   * {@code jobInstance} takes more arguments than the greatest {@code n} such
   * that the framework offers a {@code Jobn} class.
   *
   * @param <T> The return type of the child job being specified
   * @param settings
   * @param jobInstance The user-written job object
   * @param params The parameters to be passed to the {@code run} method of the
   *        job
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T> FutureValue<T> futureCallUnchecked(JobSetting[] settings, Job<?> jobInstance,
      Object... params) {
    JobRecord childJobRecord =
        PipelineManager.registerNewJobRecord(updateSpec, settings, thisJobRecord, currentRunGUID,
            jobInstance, params);
    thisJobRecord.appendChildKey(childJobRecord.getKey());
    return new FutureValueImpl<T>(childJobRecord.getOutputSlotInflated());
  }

  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to specify a job node in the generated child job graph.
   * This version of the method is for child jobs that take zero arguments.
   *
   * @param <T> The return type of the child job being specified
   * @param jobInstance A user-written job object
   * @param settings Optional {@code JobSettings}
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T> FutureValue<T> futureCall(Job0<T> jobInstance, JobSetting... settings) {
    return futureCallUnchecked(settings, jobInstance);
  }

  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to specify a job node in the generated child job graph.
   * This version of the method is for child jobs that take one argument.
   *
   * @param <T> The return type of the child job being specified
   * @param <T1> The type of the first input to the child job
   * @param jobInstance A user-written job object
   * @param v1 the first input to the child job
   * @param settings Optional {@code JobSettings}
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T, T1> FutureValue<T> futureCall(Job1<T, T1> jobInstance, Value<T1> v1,
      JobSetting... settings) {
    return futureCallUnchecked(settings, jobInstance, v1);
  }

  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to specify a job node in the generated child job graph.
   * This version of the method is for child jobs that take two arguments.
   *
   * @param <T> The return type of the child job being specified
   * @param <T1> The type of the first input to the child job
   * @param <T2> The type of the second input to the child job
   * @param jobInstance A user-written job object
   * @param v1 the first input to the child job
   * @param v2 the second input to the child job
   * @param settings Optional {@code JobSettings}
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T, T1, T2> FutureValue<T> futureCall(Job2<T, T1, T2> jobInstance, Value<T1> v1,
      Value<T2> v2, JobSetting... settings) {
    return futureCallUnchecked(settings, jobInstance, v1, v2);
  }

  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to specify a job node in the generated child job graph.
   * This version of the method is for child jobs that take three arguments.
   *
   * @param <T> The return type of the child job being specified
   * @param <T1> The type of the first input to the child job
   * @param <T2> The type of the second input to the child job
   * @param <T3> The type of the third input to the child job
   * @param jobInstance A user-written job object
   * @param v1 the first input to the child job
   * @param v2 the second input to the child job
   * @param v3 the third input to the child job
   * @param settings Optional {@code JobSettings}
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T, T1, T2, T3> FutureValue<T> futureCall(Job3<T, T1, T2, T3> jobInstance,
      Value<T1> v1, Value<T2> v2, Value<T3> v3, JobSetting... settings) {
    return futureCallUnchecked(settings, jobInstance, v1, v2, v3);
  }

  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to specify a job node in the generated child job graph.
   * This version of the method is for child jobs that take four arguments.
   *
   * @param <T> The return type of the child job being specified
   * @param <T1> The type of the first input to the child job
   * @param <T2> The type of the second input to the child job
   * @param <T3> The type of the third input to the child job
   * @param <T4> The type of the fourth input to the child job
   * @param jobInstance A user-written job object
   * @param v1 the first input to the child job
   * @param v2 the second input to the child job
   * @param v3 the third input to the child job
   * @param v4 the fourth input to the child job
   * @param settings Optional {@code JobSettings}
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T, T1, T2, T3, T4> FutureValue<T> futureCall(Job4<T, T1, T2, T3, T4> jobInstance,
      Value<T1> v1, Value<T2> v2, Value<T3> v3, Value<T4> v4, JobSetting... settings) {
    return futureCallUnchecked(settings, jobInstance, v1, v2, v3, v4);
  }


  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to specify a job node in the generated child job graph.
   * This version of the method is for child jobs that take five arguments.
   *
   * @param <T> The return type of the child job being specified
   * @param <T1> The type of the first input to the child job
   * @param <T2> The type of the second input to the child job
   * @param <T3> The type of the third input to the child job
   * @param <T4> The type of the fourth input to the child job
   * @param <T5> The type of the fifth input to the child job
   * @param jobInstance A user-written job object
   * @param v1 the first input to the child job
   * @param v2 the second input to the child job
   * @param v3 the third input to the child job
   * @param v4 the fourth input to the child job
   * @param v5 the fifth input to the child job
   * @param settings Optional {@code JobSettings}
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T, T1, T2, T3, T4, T5> FutureValue<T> futureCall(
      Job5<T, T1, T2, T3, T4, T5> jobInstance, Value<T1> v1, Value<T2> v2, Value<T3> v3,
      Value<T4> v4, Value<T5> v5, JobSetting... settings) {
    return futureCallUnchecked(settings, jobInstance, v1, v2, v3, v4, v5);
  }

  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to specify a job node in the generated child job graph.
   * This version of the method is for child jobs that take six arguments.
   *
   * @param <T> The return type of the child job being specified
   * @param <T1> The type of the first input to the child job
   * @param <T2> The type of the second input to the child job
   * @param <T3> The type of the third input to the child job
   * @param <T4> The type of the fourth input to the child job
   * @param <T5> The type of the fifth input to the child job
   * @param <T6> The type of the sixth input to the child job
   * @param jobInstance A user-written job object
   * @param v1 the first input to the child job
   * @param v2 the second input to the child job
   * @param v3 the third input to the child job
   * @param v4 the fourth input to the child job
   * @param v5 the fifth input to the child job
   * @param v6 the sixth input to the child job
   * @param settings Optional {@code JobSettings}
   * @return a {@code FutureValue} representing an empty value slot that will be
   *         filled by the output of {@code jobInstance} when it finalizes. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <T, T1, T2, T3, T4, T5, T6> FutureValue<T> futureCall(
      Job6<T, T1, T2, T3, T4, T5, T6> jobInstance, Value<T1> v1, Value<T2> v2, Value<T3> v3,
      Value<T4> v4, Value<T5> v5, Value<T6> v6, JobSetting... settings) {
    return futureCallUnchecked(settings, jobInstance, v1, v2, v3, v4, v5, v6);
  }

  /**
   * Invoke this method from within the {@code run} method of a <b>generator
   * job</b> in order to declare that some value will be provided asynchronously
   * by some external agent.
   *
   * @param <F> The type of the asynchronously provided value.
   * @param klass A {@link Class} object used to specify the type of the
   *        asynchronously provided value.
   * @return A {@code PromisedValue} that represents an empty value slot that
   *         will be filled at a later time when the external agent invokes
   *         {@link PipelineService#submitPromisedValue(String, Object)}. This
   *         may be passed in to further invocations of {@code futureCall()} in
   *         order to specify a data dependency.
   */
  protected <F> PromisedValue<F> newPromise(Class<F> klass) {
    PromisedValueImpl<F> promisedValue =
        new PromisedValueImpl<F>(getPipelineKey(), thisJobRecord.getKey(), currentRunGUID);
    updateSpec.getNonTransactionalGroup().includeSlot(promisedValue.getSlot());
    return promisedValue;
  }

  /**
   * Constructs a new {@code ImmediateValue}. This method is only syntactic
   * sugar. {@code immediate(x)} is equivalent to {@code new ImmediateValue(x)}.
   *
   * @param <F> Tye type of value wrapped by the returned {@code ImmediateValue}
   * @param value The value to be wrapped by the {@code ImmediateValue}
   * @return a new {@code ImmediateValue}
   */
  public static <F> ImmediateValue<F> immediate(F value) {
    return new ImmediateValue<F>(value);
  }

  /**
   * Constructs a new {@code WaitForSetting}. This method is only syntactic
   * sugar. {@code waitFor(fv)} is equivalent to {@code new
   * JobSetting.WaitForSetting(fv)}.
   *
   * @param fv The {@code FutureValue} to wait for
   * @return a new {@code WaitForSetting}.
   */
  public static JobSetting.WaitForSetting waitFor(FutureValue<?> fv) {
    return new JobSetting.WaitForSetting(fv);
  }

  /**
   * Constructs a new {@code JobSetting.BackoffFactor}. This method is only
   * syntactic sugar. {@code backoffFactor(x)} is equivalent to {@code new
   * JobSetting.BackoffFactor(x)}.
   *
   * @param factor The backoff factor
   * @return a new {@code JobSetting.BackoffFactor}.
   */
  public static JobSetting.BackoffFactor backOffFactor(int factor) {
    return new JobSetting.BackoffFactor(factor);
  }

  /**
   * Constructs a new {@code JobSetting.BackoffSeconds}. This method is only
   * syntactic sugar. {@code backoffSeconds(x)} is equivalent to {@code new
   * JobSetting.BackoffSeconds(x)}.
   *
   * @param seconds The backoff seconds
   * @return a new {@code JobSetting.BackoffSeconds}.
   */
  public static JobSetting.BackoffSeconds backOffSeconds(int seconds) {
    return new JobSetting.BackoffSeconds(seconds);
  }

  /**
   * Constructs a new {@code JobSetting.MaxAttempts}. This method is only
   * syntactic sugar. {@code maxAttempts(x)} is equivalent to {@code new
   * JobSetting.MaxAttempts(x)}.
   *
   * @param attempts The maximum number of attempts
   * @return a new {@code JobSetting.BackoffSeconds}.
   */
  public static JobSetting.MaxAttempts maxAttempts(int attempts) {
    return new JobSetting.MaxAttempts(attempts);
  }

  /**
   * Constructs a new {@code JobSetting.OnBackend}. This method is only
   * syntactic sugar. {@code onBackend(x)} is equivalent to
   * {@code new JobSetting.OnBackend(x)}.
   */
  public static JobSetting.OnBackend onBackend(String backend) {
    return new JobSetting.OnBackend(backend);
  }

  /**
   * Constructs a new {@code FutureList}. This method is only syntactic sugar.
   * {@code futureList(listOfValues)} is equivalent to {@code new
   * FutureList(listOfValues)}.
   *
   * @param <F> The type of element in the list
   * @param listOfValues A list of {@code Values<F>}
   * @return A new {@code FutureList<F>}.
   */
  public static <F> FutureList<F> futureList(List<? extends Value<F>> listOfValues) {
    return new FutureList<F>(listOfValues);
  }

  /**
   * Returns the Key uniquely identifying this job
   *
   * @return the Key uniquely identifying this job
   */
  protected Key getJobKey() {
    return thisJobRecord.getKey();
  }

  /**
   * Returns the Key uniquely identifying the Pipeline that this job is a member
   * of This is the same as the Key of the root Job of the Pipeline.
   *
   * @return the Key uniquely identifying the Pipeline that this job is a member
   *         of
   */
  protected Key getPipelineKey() {
    return thisJobRecord.getRootJobKey();
  }

}
