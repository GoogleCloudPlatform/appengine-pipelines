// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;


/**
 * A setting for specifying to the framework some aspect of a Job's execution.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface JobSetting {
  
  /**
   * A setting for specifying that a Job should not be run
   * until the value slot represented by the given {@code FutureValue}
   * has been filled.
   */
  final class WaitForSetting implements JobSetting {
    private FutureValue<?> futureValue;

    public WaitForSetting(FutureValue<?> fv) {
      this.futureValue = fv;
    }

    public FutureValue<?> getFutureValue() {
      return futureValue;
    }
  }
  
  /**
   * An abstract parent object for integer settings
   */
  abstract class IntValuedSetting implements JobSetting {
    private int value;
    protected IntValuedSetting(int val){
      this.value = val;
    }
    public int getValue(){
      return value;
    }
  }
  
  /**
   * A setting for specifying how long to wait
   * before retrying a failed job. The wait time
   * will be 
   * <pre>
   * <code>
   * backoffSeconds * backoffFactor ^ attemptNumber
   * </code>
   * </pre>
   *
   */
  final class BackoffSeconds extends IntValuedSetting {
    public static final int DEFAULT = 15;
    public BackoffSeconds(int seconds){
      super(seconds);
    }
  }
  
  /**
   * A setting for specifying how long to wait
   * before retrying a failed job. The wait time
   * will be 
   * <pre>
   * <code>
   * backoffSeconds * backoffFactor ^ attemptNumber
   * </code>
   * </pre>
   *
   */
  final class BackoffFactor extends IntValuedSetting {
    public static final int DEFAULT = 2;
    public BackoffFactor(int factor){
      super(factor);
    }
  }
  
  /**
   * A setting for specifying how many times to retry
   * a failed job.
   */
  final class MaxAttempts extends IntValuedSetting {
    public static final int DEFAULT = 3;
    public MaxAttempts(int attempts){
      super(attempts);
    }
  }
}
