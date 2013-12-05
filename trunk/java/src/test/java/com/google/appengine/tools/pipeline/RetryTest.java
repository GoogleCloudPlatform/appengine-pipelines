// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline;

import static com.google.appengine.tools.pipeline.impl.util.GUIDGenerator.USE_SIMPLE_GUIDS_FOR_DEBUGGING;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class RetryTest extends TestCase {

  private LocalServiceTestHelper helper;

  public RetryTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), taskQueueConfig,
        new LocalModulesServiceTestConfig());
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  private static volatile CountDownLatch countdownLatch;

  public void testMaxAttempts() throws Exception {
    doMaxAttemptsTest(true);
    doMaxAttemptsTest(false);
  }

  public void testLongBackoffTime() throws Exception {
    // Fail twice with a 3 second backoff factor. Wait 5 seconds. Should
    // succeed.
    runJob(3, 2, 5, false);

    // Fail 3 times with a 3 second backoff factor. Wait 10 seconds. Should fail
    // because 3 + 9 = 12 > 10
    try {
      runJob(3, 3, 10, false);
      fail("Excepted exception");
    } catch (AssertionFailedError e) {
      // expected;
    }

    // Fail 3 times with a 3 second backoff factor. Wait 15 seconds. Should
    // succeed
    // because 3 + 9 = 12 < 15
    runJob(3, 3, 15, false);
  }

  private void doMaxAttemptsTest(boolean succeedTheLastTime) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = runJob(1, 4, 10, succeedTheLastTime);
    // Wait for framework to save Job information
    Thread.sleep(1000L);
    JobInfo jobInfo = service.getJobInfo(pipelineId);
    JobInfo.State expectedState =
        (succeedTheLastTime ? JobInfo.State.COMPLETED_SUCCESSFULLY
            : JobInfo.State.STOPPED_BY_ERROR);
    assertEquals(expectedState, jobInfo.getJobState());
  }

  private String runJob(int backoffFactor, int maxAttempts, int awaitSeconds,
      boolean succeedTheLastTime) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    countdownLatch = new CountDownLatch(maxAttempts);

    String pipelineId = service.startNewPipeline(
        new InvokesFailureJob(succeedTheLastTime, maxAttempts, backoffFactor));
    assertTrue(countdownLatch.await(awaitSeconds, TimeUnit.SECONDS));
    return pipelineId;
  }

  /**
   * A job that invokes {@link FailureJob}.
   */
  @SuppressWarnings("serial")
  public static class InvokesFailureJob extends Job0<Void> {
    private boolean succeedTheLastTime;
    int maxAttempts;
    int backoffFactor;

    public InvokesFailureJob(boolean succeedTheLastTime, int maxAttempts, int backoffFactor) {
      this.succeedTheLastTime = succeedTheLastTime;
      this.maxAttempts = maxAttempts;
      this.backoffFactor = backoffFactor;
    }

    @Override
    public Value<Void> run() {
      JobSetting[] jobSettings =
          new JobSetting[] {new MaxAttempts(maxAttempts), new BackoffSeconds(1),
              new BackoffFactor(backoffFactor)};
      return futureCall(new FailureJob(succeedTheLastTime), jobSettings);
    }
  }

  /**
   * A job that fails every time except possibly the last time.
   */
  @SuppressWarnings("serial")
  public static class FailureJob extends Job0<Void> {
    private boolean succeedTheLastTime;

    public FailureJob(boolean succeedTheLastTime) {
      this.succeedTheLastTime = succeedTheLastTime;
    }

    @Override
    public Value<Void> run() {
      countdownLatch.countDown();
      if (countdownLatch.getCount() == 0 && succeedTheLastTime) {
        return null;
      }
      throw new RuntimeException("Hello");
    }
  }
}
