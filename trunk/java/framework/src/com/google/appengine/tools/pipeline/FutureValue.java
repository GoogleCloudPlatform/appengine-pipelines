package com.google.appengine.tools.pipeline;


/**
 * An abstract representation of a value slot that will be
 * filled in the future when some job runs and produces output.
 * <p>
 * An instance of {@code FutureValue} is obtained as the return
 * value of a call to one of the {@code futureCall(job, argument...)} family
 * of methods. The returned {@code FutureValue} represents the
 * future output from the given job.
 * <p>
 * Once obtained in this way, an instance of {@code FutureValue}
 * may be passed as an argument to another invocation
 * of {@code futureCall(anotherJob, argumnet...)} indicating that the output from
 * the first job should be an input to the second job. For example
 * the following code might appear inside of the {@code run()}
 * method of a Job.
 * <blockquote>
 * <pre>
 * FutureValue<Integer> age = futureCall(new GetAgeJob());
 * futureCall(new UseAgeJob(), age);
 * </pre>
 * </blockquote>
 * The invocation of {@code futureCall(new UseAgeJob(), age)}  above
 * instructs the framework that after the value slot corresponding to age has been filled,
 * the {@code run()} method of {@code UseAgeJob}
 * should be invoked with the value of the filled slot in the argument
 * position corresponding to {@code age}.
 * <p>
 * A {@code FutureValue} may also be the return value
 * of the {@code run()} method.
 * 
 * @param <E> The type of the value represented by this {@code
 *        FutureValue}
 * */
public interface FutureValue<E> extends Value<E> {
  
  /**
   * Returns a String uniquely identifying the Job whose output will
   * fill the value slot represented by this {@code FutureValue}. This
   * String may be passed to {@link PipelineService#getJobInfo(String)}
   * in order to query the state of the Job.
   * @return a String uniquely identifying the source job.
   */
  public String getSourceJobHandle();
  
  /**
   * Returns a String uniquely identifying the Pipeline that this {@code FutureValue}
   * belongs to. This is the same as the handle of the root job of the Pipeline.
   * This String may be passed to {@link PipelineService#getJobInfo(String)}
   * in order to query the state of the root Job.
   * @return a String uniquely identifying the Pipeline 
   */
  public String getPipelineHandle();
  
}
