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
import com.google.apphosting.api.ApiProxy;

import junit.framework.TestCase;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class PipelineTest extends TestCase {

  protected transient LocalServiceTestHelper helper;
  protected transient ApiProxy.Environment apiProxyEnvironment;

  private static StringBuffer traceBuffer;

  public PipelineTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(
        new LocalDatastoreServiceTestConfig()
            .setDefaultHighRepJobPolicyUnappliedJobPercentage(
                isHrdSafe() ? 100 : 0),
        taskQueueConfig, new LocalModulesServiceTestConfig());
  }

  /**
   * Whether this test will succeed even if jobs remain unapplied indefinitely.
   *
   * NOTE: This may be called from the constructor, i.e., before the object is
   * fully initialized.
   */
  protected boolean isHrdSafe() {
    return true;
  }

  protected static void trace(String what) {
    if (traceBuffer.length() > 0) {
      traceBuffer.append(' ');
    }
    traceBuffer.append(what);
  }

  protected static String trace() {
    return traceBuffer.toString();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    traceBuffer = new StringBuffer();
    helper.setUp();
    apiProxyEnvironment = ApiProxy.getCurrentEnvironment();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  protected Object waitForJobToComplete(String pipelineId) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    while (true) {
      Thread.sleep(2000);
      JobInfo jobInfo = service.getJobInfo(pipelineId);
      switch (jobInfo.getJobState()) {
        case COMPLETED_SUCCESSFULLY:
          return jobInfo.getOutput();
        case RUNNING:
          break;
        case STOPPED_BY_ERROR:
          throw new RuntimeException("Job stopped " + jobInfo.getError());
        case STOPPED_BY_REQUEST:
          throw new RuntimeException("Job stopped by request.");
        case CANCELED_BY_REQUEST:
          throw new RuntimeException("Job was canceled by request.");
        default:
          break;
      }
    }
  }
}
