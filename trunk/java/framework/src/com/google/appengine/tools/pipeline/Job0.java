package com.google.appengine.tools.pipeline;

/**
 * A user's job class should subclass a specialization of this class if the job
 * does not take any input parameters.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 * @param <T> The type of the job's output.
 */
public abstract class Job0<T> extends Job<T> {

  /**
   * User's must define this method in their job class.
   */
  public abstract Value<T> run();
}
