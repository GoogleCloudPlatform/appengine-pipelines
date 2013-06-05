// Copyright 2013 Google Inc.
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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test error handling through handleException.
 * 
 * @author maximf@google.com (Maxim Fateev)
 */
public class DelayedValueTest extends PipelineTest {

  private static final int EXPECTED_RESULT = 5;

  private PipelineService service = PipelineServiceFactory.newPipelineService();

  private static AtomicLong duration = new AtomicLong();
  
  static class DelayedJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() throws Exception {
      trace("DelayedJob.run");
      duration.set(System.currentTimeMillis() - duration.get());
      return immediate(EXPECTED_RESULT);
    }
    
  }
  private static final long DELAY_SECONDS = 3;

  static class TestDelayedValueJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestDelayedValueJob.run");
      duration.set(System.currentTimeMillis());
      Value<Void> delayedValue = newDelayedValue(DELAY_SECONDS);
      return futureCall(new DelayedJob(), waitFor(delayedValue));
    }
    
  }
  
  public void testDelayedValue() throws Exception {
    String pipelineId = service.startNewPipeline(new TestDelayedValueJob());
    Integer five = (Integer) waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT, five.intValue());
    assertEquals("TestDelayedValueJob.run DelayedJob.run", trace());
    assertTrue(duration.get() >= DELAY_SECONDS * 1000);
  }
}
