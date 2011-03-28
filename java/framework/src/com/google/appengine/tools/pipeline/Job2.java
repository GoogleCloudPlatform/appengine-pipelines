package com.google.appengine.tools.pipeline;

/**
 * A user's job class should subclass a specialization of this class if the job
 * takes two input parameter.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 * @param <T> The type of the job's output.
 * @param <T1> The type of the job's first input.
 * @param <T2> The type of the job's second input.
 */
public abstract class Job2<T, T1, T2> extends Job<T> {

  /**
   * User's must define this method in their job class.
   */
  public abstract Value<T> run(T1 param1, T2 param2);
}
