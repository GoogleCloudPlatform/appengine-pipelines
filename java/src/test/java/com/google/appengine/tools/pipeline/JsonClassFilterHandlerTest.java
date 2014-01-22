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

import static org.mockito.Mockito.when;

import com.google.appengine.tools.pipeline.impl.servlets.JsonClassFilterHandler;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Test for {@link JsonClassFilterHandler}.
 */
public class JsonClassFilterHandlerTest extends PipelineTest {

  @Mock private HttpServletRequest request;
  @Mock private HttpServletResponse response;
  private final StringWriter output = new StringWriter();

  @SuppressWarnings("serial")
  private static class Main1Job extends Job0<String> {

    @Override
    public Value<String> run() {
      FutureValue<String> v1 = futureCall(new StrJob<String>(), immediate("hello"));
      FutureValue<String> v2 = futureCall(new StrJob<String>(), immediate(" world"));
      return futureCall(new ConcatJob(), v1, v2);
    }
  }

  @SuppressWarnings("serial")
  private static class Main2Job extends Job0<String> {

    private final boolean shouldThrow;

    public Main2Job(boolean shouldThrow) {
      this.shouldThrow = shouldThrow;
    }

    @Override
    public Value<String> run() {
      if (shouldThrow) {
        throw new RuntimeException("bla");
      }
      FutureValue<String> v1 = futureCall(new StrJob<String>(), immediate("hi"));
      FutureValue<String> v2 = futureCall(new StrJob<String>(), immediate(" there"));
      return futureCall(new ConcatJob(), v1, v2);
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(Throwable t) {
      return immediate(t.getMessage());
    }
  }

  @SuppressWarnings("serial")
  private static class ConcatJob extends Job2<String, String, String> {

    @Override
    public Value<String> run(String value1, String value2) {
      return immediate(value1 + value2);
    }
  }

  @SuppressWarnings("serial")
  private static class StrJob<T extends Serializable> extends Job1<String, T> {

    @Override
    public Value<String> run(T obj) {
      return immediate(obj == null ? "null" : obj.toString());
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    MockitoAnnotations.initMocks(this);
    when(response.getWriter()).thenReturn(new PrintWriter(output));
  }

  public void testHandlerNoResults() throws Exception {
    JsonClassFilterHandler.doGet(request, response);
    assertEquals("{\"classPaths\": []}", output.toString());
  }

  public void testHandlerWithResults() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId1 = service.startNewPipeline(new Main1Job());
    String pipelineId2 = service.startNewPipeline(new Main2Job(false));
    String pipelineId3 = service.startNewPipeline(new Main2Job(true),
        new JobSetting.BackoffSeconds(0), new JobSetting.MaxAttempts(2));
    String helloWorld = (String) waitForJobToComplete(pipelineId1);
    assertEquals("hello world", helloWorld);
    String hiThere = (String) waitForJobToComplete(pipelineId2);
    assertEquals("hi there", hiThere);
    String bla = (String) waitForJobToComplete(pipelineId3);
    assertEquals("bla", bla);
    JsonClassFilterHandler.doGet(request, response);
    System.out.println(output.toString());
    String expected = "{\"classPaths\": [\n"
        + "  \"" + Main1Job.class.getName() + "\",\n"
        + "  \"" + Main2Job.class.getName() + "\"\n"
        + "]}";
    assertEquals(expected, output.toString());
  }
}
