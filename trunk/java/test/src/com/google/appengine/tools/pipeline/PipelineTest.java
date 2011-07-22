// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.demo.AsyncGCDExample;
import com.google.appengine.tools.pipeline.demo.AsyncGCDExample.PrintGCDJob;
import com.google.appengine.tools.pipeline.demo.GCDExample.GCDJob;
import com.google.appengine.tools.pipeline.demo.LetterCountExample;
import com.google.appengine.tools.pipeline.demo.LetterCountExample.LetterCounter;
import com.google.apphosting.api.ApiProxy;

import junit.framework.TestCase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class PipelineTest extends TestCase implements Serializable {

  private static transient final Logger logger = Logger.getLogger(PipelineTest.class.getName());

  protected transient LocalServiceTestHelper helper;
  protected transient ApiProxy.Environment apiProxyEnvironment;

  public PipelineTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), taskQueueConfig);

  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    apiProxyEnvironment = ApiProxy.getCurrentEnvironment();
    System.setProperty("com.google.appengine.api.pipeline.use-simple-guids-for-debugging", "true");
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  @SuppressWarnings("unchecked")
  protected <E> E waitForJobToComplete(String pipelineId) throws Exception {
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
