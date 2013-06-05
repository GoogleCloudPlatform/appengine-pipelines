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

/**
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class MiscPipelineTest extends PipelineTest {

  public void testImmediateChild() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new Returns5FromChildJob());
    Integer five = (Integer) waitForJobToComplete(pipelineId);
    assertEquals(5, five.intValue());
  }

  private static class Returns5FromChildJob extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      return futureCall(new IdentityJob(), immediate(5));
    }
  }

  private static class IdentityJob extends Job1<Integer, Integer> {
    @Override
    public Value<Integer> run(Integer param1) {
      return immediate(param1);
    }
  }

}
