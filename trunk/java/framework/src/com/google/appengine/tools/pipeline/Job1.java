package com.google.appengine.tools.pipeline;

/**
 * A user's job class should subclass a specialization of this class if the job
 * takes one input parameter.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 * @param <T> The type of the job's output.
 * @param <T1> The type of the job's first input.
 */
public abstract class Job1<T, T1> extends Job<T> {

  /**
   * User's must define this method in their job class.
   */
  public abstract Value<T> run(T1 param1);
}
