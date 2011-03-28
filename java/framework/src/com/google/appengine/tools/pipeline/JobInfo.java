// Copyright 2010 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

/**
 * A record about a job that has been registered with the framework.
 * A {@code JobInfo} is obtained via the method {@link PipelineService#getJobInfo(String)}.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface JobInfo {
  
  /**
   * The state of the job.
   */
  static enum State {RUNNING, COMPLETED_SUCCESSFULLY, STOPPED_BY_REQUEST, STOPPED_BY_ERROR, WAITING_TO_RETRY}
  
  /**
   * Get the job's {@link State}.
   */
  State getJobState();
  
  /**
   * If the job's {@link State State} is 
   * {@link State#COMPLETED_SUCCESSFULLY COMPLETED_SUCCESSFULLY},
   * returns the job's output. Otherwise returns {@code null}.
   */
  Object getOutput();
  
  /**
   * If the job's {@link State State} is 
   * {@link State#STOPPED_BY_ERROR STOPPED_BY_ERROR},
   * returns the error stack trace. Otherwise returns {@code null}
   */
  String getError();

}
