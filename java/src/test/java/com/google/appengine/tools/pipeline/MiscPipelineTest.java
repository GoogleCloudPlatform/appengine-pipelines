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

import java.util.Arrays;
import java.util.Random;

/**
 * Misc tests including passing large values.

 * @author rudominer@google.com (Mitch Rudominer)
 */
public class MiscPipelineTest extends PipelineTest {

  private static long[] largeValue;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    largeValue = new long[2000000];
    Random random = new Random();
    for (int i = 0; i < largeValue.length; i++) {
      largeValue[i] = random.nextLong();
    }
  }

  public void testImmediateChild() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new Returns5FromChildJob(), largeValue);
    Integer five = (Integer) waitForJobToComplete(pipelineId);
    assertEquals(5, five.intValue());
  }

  @SuppressWarnings("serial")
  private static class Returns5FromChildJob extends Job1<Integer, long[]> {
    @Override
    public Value<Integer> run(long[] bytes) {
      assertTrue(Arrays.equals(largeValue, bytes));
      FutureValue<Integer> lengthJob = futureCall(new LengthJob(), immediate(bytes));
      return futureCall(new IdentityJob(bytes), immediate(5), lengthJob);
    }
  }

  @SuppressWarnings("serial")
  private static class IdentityJob extends Job2<Integer, Integer, Integer> {

    private final long[] bytes;

    public IdentityJob(long[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public Value<Integer> run(Integer param1, Integer length) {
      assertEquals(largeValue.length, length.intValue());
      assertTrue(Arrays.equals(largeValue, bytes));
      return immediate(param1);
    }
  }

  @SuppressWarnings("serial")
  private static class LengthJob extends Job1<Integer, long[]> {
    @Override
    public Value<Integer> run(long[] bytes) {
      assertTrue(Arrays.equals(largeValue, bytes));
      return immediate(bytes.length);
    }
  }
}
