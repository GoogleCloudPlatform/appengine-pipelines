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

  private transient LocalServiceTestHelper helper;
  private transient ApiProxy.Environment apiProxyEnvironment;

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

  public void testGCDCalculation() throws Exception {
    doGcdTest(1, 1, 1);
    doGcdTest(12, 20, 4);
    doGcdTest(3600, 105, 15);
  }

  public void testAsyncGCD() throws Exception {
    doAsyncGcdTest("Sparkles", 2, 3, "Hello, Sparkles. The GCD of 2 and 3 is 1.");
    doAsyncGcdTest("Biff", 2, 2, "Hello, Biff. The GCD of 2 and 2 is 2.");
  }

  public void testLetterCounter() throws Exception {
    doLetterCounterTest("Only three words.");
    doLetterCounterTest("The quick brown fox jumps over the lazy dog.");
    doLetterCounterTest("The woods are lovely dark and deep. " + "But I have promises to keep. "
        + "And miles to go before I sleep.");
  }
  
  public void testFutureList() throws Exception {
   
    Job0<Integer> testJob = new Job0<Integer>() {
      @Override
      public Value<Integer> run() {
        
        Job0<Integer> returns5Job = new Job0<Integer>() {
          @Override
          public Value<Integer> run() {
            return immediate(5);
          }
        };
        
        Job1<Integer, List<Integer>> sumJob = new Job1<Integer, List<Integer>>()  {
          @Override
          public Value<Integer> run(List<Integer> list){
            int sum = 0;
            for(int x : list){
              sum += x;
            }
            return immediate(sum);
          }
        };
        
        List<Value<Integer>> valueList = new ArrayList<Value<Integer>>(4);
        valueList.add(futureCall(returns5Job));
        valueList.add(immediate(7));
        valueList.add(futureCall(returns5Job));
        valueList.add(immediate(4));
        
        return futureCall(sumJob, futureList(valueList));
      }
    };
    
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(testJob);
    Integer sum = waitForJobToComplete(pipelineId);
    assertEquals(21, sum.intValue());
  }


  private void doGcdTest(int x, int y, int expectedGcd) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new GCDJob(), x, y);
    int calculatedGcd = waitForJobToComplete(pipelineId);
    logger.info("The GCD of " + x + " and " + y + " is " + calculatedGcd);
    assertEquals(expectedGcd, calculatedGcd);
    // PipelineObjects pipelineObjects =
    // PipelineManager.queryFullPipeline(pipelineId);
    // System.out.println(pipelineObjects.toJson());
  }

  private void doLetterCounterTest(String text) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new LetterCounter(), text);
    SortedMap<Character, Integer> counts = waitForJobToComplete(pipelineId);
    SortedMap<Character, Integer> expectedCounts = LetterCountExample.countLetters(text);
    SortedMap<Character, Integer> expectedCountsLettersOnly = new TreeMap<Character, Integer>();
    for (Entry<Character, Integer> entry : expectedCounts.entrySet()) {
      if (Character.isLetter(entry.getKey())) {
        expectedCountsLettersOnly.put(entry.getKey(), entry.getValue());
      }
    }
    assertEquals(expectedCountsLettersOnly, counts);
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

  private void doAsyncGcdTest(
      final String userName, final int x, final int y, String expectedMessage) throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final StringBuilder builder = new StringBuilder();
    AsyncGCDExample.callback = new AsyncGCDExample.Callback() {
      @Override
      public String getUserName() {
        ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
        return userName;
      }

      @Override
      public int getSecondInt() {
        ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
        return y;
      }

      @Override
      public int getFirstInt() {
        ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
        return x;
      }

      @Override
      public void acceptOutput(String output) {
        builder.append(output);
        latch.countDown();
      }
    };
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new PrintGCDJob());
    latch.await(3, TimeUnit.MINUTES);
    assertEquals(expectedMessage, builder.toString());
    // Wait for job task thread to complete
    Thread.sleep(2000);
    JobInfo jobInfo = service.getJobInfo(pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
  }

}
