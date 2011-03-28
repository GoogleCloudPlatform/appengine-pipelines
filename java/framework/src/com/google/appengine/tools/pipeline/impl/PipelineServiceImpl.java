package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.Job3;
import com.google.appengine.tools.pipeline.Job4;
import com.google.appengine.tools.pipeline.Job5;
import com.google.appengine.tools.pipeline.Job6;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineService;

public class PipelineServiceImpl implements PipelineService {


  public String startNewPipeline(Job0<?> jobInstance, JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance);
  }

  public <T1> String startNewPipeline(Job1<?, T1> jobInstance, T1 arg1, JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance, arg1);
  }

  public <T1, T2> String startNewPipeline(
      Job2<?, T1, T2> jobInstance, T1 arg1, T2 arg2, JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance, arg1, arg2);
  }

  public <T1, T2, T3> String startNewPipeline(
      Job3<?, T1, T2, T3> jobInstance, T1 arg1, T2 arg2, T3 arg3, JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance, arg1, arg2, arg3);
  }

  public <T1, T2, T3, T4> String startNewPipeline(Job4<?, T1, T2, T3, T4> jobInstance,
      T1 arg1,
      T2 arg2,
      T3 arg3,
      T4 arg4,
      JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance, arg1, arg2, arg3, arg4);
  }

  public <T1, T2, T3, T4, T5> String startNewPipeline(Job5<?, T1, T2, T3, T4, T5> jobInstance,
      T1 arg1,
      T2 arg2,
      T3 arg3,
      T4 arg4,
      T5 arg5,
      JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance, arg1, arg2, arg3, arg4, arg5);
  }

  public <T1, T2, T3, T4, T5, T6> String startNewPipeline(Job6<
      ?, T1, T2, T3, T4, T5, T6> jobInstance,
      T1 arg1,
      T2 arg2,
      T3 arg3,
      T4 arg4,
      T5 arg5,
      T6 arg6,
      JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance, arg1, arg2, arg3, arg4, arg5);
  }

  public String startNewPipelineUnchecked(
      Job<?> jobInstance, Object[] arguments, JobSetting... settings) {
    return PipelineManager.startNewCascade(settings, jobInstance, arguments);
  }

  public void stopPipeline(String jobHandle) throws NoSuchObjectException {
    PipelineManager.stopJob(jobHandle);
  }

  public void deletePipelineRecords(String pipelineHandle)
      throws NoSuchObjectException, IllegalStateException {
    PipelineManager.deletePipelineRecords(pipelineHandle);
  }

  public JobInfo getJobInfo(String jobHandle) throws NoSuchObjectException {
    return PipelineManager.getJob(jobHandle);
  }

  public void submitPromisedValue(String promiseHandle, Object value) throws NoSuchObjectException {
    PipelineManager.acceptPromisedValue(promiseHandle, value);
  }
}
