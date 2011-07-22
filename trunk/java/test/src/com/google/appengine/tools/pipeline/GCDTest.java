package com.google.appengine.tools.pipeline;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.appengine.tools.pipeline.AsyncGCDExample.PrintGCDJob;
import com.google.appengine.tools.pipeline.demo.GCDExample.GCDJob;
import com.google.apphosting.api.ApiProxy;

public class GCDTest extends PipelineTest {

  private static transient final Logger logger = Logger.getLogger(GCDTest.class.getName());

  public void testGCDCalculation() throws Exception {
    doGcdTest(1, 1, 1);
    doGcdTest(12, 20, 4);
    doGcdTest(3600, 105, 15);
  }

  public void testAsyncGCD() throws Exception {
    doAsyncGcdTest("Sparkles", 2, 3, "Hello, Sparkles. The GCD of 2 and 3 is 1.");
    doAsyncGcdTest("Biff", 2, 2, "Hello, Biff. The GCD of 2 and 2 is 2.");
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

  private void doAsyncGcdTest(final String userName, final int x, final int y,
      String expectedMessage) throws Exception {
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
