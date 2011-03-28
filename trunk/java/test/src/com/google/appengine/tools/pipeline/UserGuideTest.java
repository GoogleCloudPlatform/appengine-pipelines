// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.demo.UserGuideExamples.ComplexJob;

import junit.framework.TestCase;

/**
 * Tests for the sample code in the User Guide
 *
 * @author rudominer@google.com (Mitch Rudominer
 */
public class UserGuideTest extends TestCase {

  private transient LocalServiceTestHelper helper;

  public UserGuideTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldPushApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), taskQueueConfig);

  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    System.setProperty("com.google.appengine.api.pipeline.use-simple-guids-for-debugging", "true");
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testComplexJob() throws Exception {
    doComplexJobTest(3, 7, 11);
    doComplexJobTest(-5, 71, 6);
  }

  private void doComplexJobTest(int x, int y, int z) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new ComplexJob(), x, y, z);
    JobInfo jobInfo = service.getJobInfo(pipelineId);
    JobInfo.State state = jobInfo.getJobState();
    if (JobInfo.State.COMPLETED_SUCCESSFULLY == state){
      System.out.println("The output is " + jobInfo.getOutput());
    }
    int output = waitForJobToComplete(pipelineId);
    assertEquals(((x - y) * (x - z)) - 2, output);
  }

  @SuppressWarnings("unchecked")
  private <E> E waitForJobToComplete(String pipelineId) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    while (true) {
      Thread.sleep(2000);
      JobInfo jobInfo = service.getJobInfo(pipelineId);
      switch (jobInfo.getJobState()) {
        case COMPLETED_SUCCESSFULLY:
          return (E) jobInfo.getOutput();
        case RUNNING:
          break;
        case STOPPED_BY_ERROR:
          throw new RuntimeException("Job stopped " + jobInfo.getError());
        case STOPPED_BY_REQUEST:
          throw new RuntimeException("Job stopped by request.");
      }
    }
  }
}
