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

import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.impl.PipelineManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    // This is also testing inheritance of statusConsoleUrl.
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new Returns5FromChildJob(), largeValue);
    Integer five = (Integer) waitForJobToComplete(pipelineId);
    assertEquals(5, five.intValue());
  }

  public void testPromisedValue() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new FillPromisedValueJob());
    String helloWorld = (String) waitForJobToComplete(pipelineId);
    assertEquals("hello world", helloWorld);
  }

  public void testDelayedValueInSlowJob() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new UsingDelayedValueInSlowJob());
    String hello = (String) waitForJobToComplete(pipelineId);
    assertEquals("I am delayed", hello);
  }

  @SuppressWarnings("serial")
  static class Temp<T extends Serializable> implements Serializable {

    private T value;

    Temp(T value) {
      this.value = value;
    }

    T getValue() {
      return value;
    }
  }

  @SuppressWarnings("serial")
  private static class FillPromisedValueJob extends Job0<String> {

    @Override
    public Value<String> run() {
      PromisedValue<List<Temp<String>>> ps = newPromise();
      futureCall(new PopulatePromisedValueJob(), immediate(ps.getHandle()));
      return futureCall(new ConsumePromisedValueJob(), ps);
    }
  }

  @SuppressWarnings("serial")
  private static class PopulatePromisedValueJob extends Job1<Void, String> {

    @Override
    public Value<Void> run(String handle) throws NoSuchObjectException, OrphanedObjectException {
      List<Temp<String>> list = new ArrayList<>();
      list.add(new Temp<>("hello"));
      list.add(new Temp<>(" "));
      list.add(new Temp<>("world"));
      PipelineManager.acceptPromisedValue(handle, list);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class ConsumePromisedValueJob extends Job1<String, List<Temp<String>>> {

    @Override
    public Value<String> run(List<Temp<String>> values) {
      String value = "";
      for (Temp<String> temp : values) {
        value += temp.getValue();
      }
      return immediate(value);
    }
  }

  @SuppressWarnings("serial")
  private static class StrJob<T extends Serializable> extends Job1<String, T> {

    @Override
    public Value<String> run(T obj) {
      return immediate(obj == null ? "null" : obj.toString());
    }
  }

  @SuppressWarnings("serial")
  private static class UsingDelayedValueInSlowJob extends Job0<String> {

    @Override
    public Value<String> run() throws InterruptedException {
      Value<Void> delayedValue = newDelayedValue(1);
      Thread.sleep(3000);
      // We would like to validate that the delay will work even when used after
      // its delayed value. It used to fail before, b/12081152, but now it should
      // pass (and semantic of the delay was changed, so delay value starts only
      // after this run method completes.
      return futureCall(new StrJob<>(), immediate("I am delayed"), waitFor(delayedValue));
    }
  }

  @SuppressWarnings("serial")
  private static class Returns5FromChildJob extends Job1<Integer, long[]> {
    @Override
    public Value<Integer> run(long[] bytes) {
      setStatusConsoleUrl("my-console-url");
      assertTrue(Arrays.equals(largeValue, bytes));
      FutureValue<Integer> lengthJob = futureCall(new LengthJob(), immediate(bytes));
      return futureCall(
          new IdentityJob(bytes), immediate(5), lengthJob, new StatusConsoleUrl(null));
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
      assertNull(getStatusConsoleUrl());
      assertEquals(largeValue.length, length.intValue());
      assertTrue(Arrays.equals(largeValue, bytes));
      return immediate(param1);
    }
  }

  @SuppressWarnings("serial")
  private static class LengthJob extends Job1<Integer, long[]> {
    @Override
    public Value<Integer> run(long[] bytes) {
      assertEquals("my-console-url", getStatusConsoleUrl());
      assertTrue(Arrays.equals(largeValue, bytes));
      return immediate(bytes.length);
    }
  }
}
