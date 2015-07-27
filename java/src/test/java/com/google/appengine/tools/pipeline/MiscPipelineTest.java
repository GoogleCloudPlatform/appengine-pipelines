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

import com.google.appengine.tools.pipeline.JobInfo.State;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Misc tests including:
 *  Passing large values.
 *  Delay used in a slow job
 *  JobSetting inheritance
 *  PromisedValue (and filling it more than once)
 *  Cancel pipeline
 *  WaitFor passed to a new pipeline
 *  FutureValue passed to a new pipeline
 *
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

  @Override
  protected boolean isHrdSafe() {
    return false;
  }

  @SuppressWarnings("serial")
  private static class ReturnFutureListJob extends Job0<List<String>> {

    @Override
    public Value<List<String>> run() throws Exception {
      FutureValue<String> child1 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      FutureValue<String> child2 = futureCall(new StrJob<>(), immediate(Long.valueOf(456)));
      return new FutureList<>(ImmutableList.of(child1, child2));
    }
  }

  public void testReturnFutureList() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new ReturnFutureListJob());
    List<String> value = waitForJobToComplete(pipelineId);
    assertEquals(ImmutableList.of("123", "456"), value);
  }

  private static class StringToLong implements Function<String, Long>, Serializable {

    private static final long serialVersionUID = -913828405842203610L;

    @Override
    public Long apply(String input) {
      return Long.valueOf(input);
    }
  }

  @SuppressWarnings("serial")
  private static class TestTransformJob extends Job0<Long> {

    @Override
    public Value<Long> run() {
      FutureValue<String> child1 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      return futureCall(new Jobs.Transform<>(new StringToLong()), child1);
    }
  }

  public void testTransform() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new TestTransformJob());
    Long value = waitForJobToComplete(pipelineId);
    assertEquals(Long.valueOf(123), value);
  }


  @SuppressWarnings("serial")
  private static class RootJob extends Job0<String> {

    private final boolean delete;

    RootJob(boolean delete) {
      this.delete = delete;
    }

    @Override
    public Value<String> run() throws Exception {
      FutureValue<String> child1 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      FutureValue<String> child2 = futureCall(new StrJob<>(), immediate(Long.valueOf(456)));
      if (delete) {
        return Jobs.waitForAllAndDelete(this, child1, child2);
      } else {
        return Jobs.waitForAll(this, child1, child2);
      }
    }
  }

  public void testWaitForAll() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new RootJob(false));
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertEquals("123", jobInfo.getOutput());
    assertNotNull(service.getJobInfo(pipelineId));
  }

  public void testWaitForAllAndDelete() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new RootJob(true));
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertEquals("123", jobInfo.getOutput());
    waitUntilTaskQueueIsEmpty();
    try {
      service.getJobInfo(pipelineId);
      fail("Was expecting a NoSuchObjectException exception");
    } catch (NoSuchObjectException expected) {
      // expected;
    }
  }

  @SuppressWarnings("serial")
  private static class CallerJob extends Job0<String> {

    static AtomicBoolean flag = new AtomicBoolean();

    @Override
    public Value<String> run() throws Exception {
      FutureValue<Void> child1 = futureCall(new ChildJob());
      FutureValue<String> child2 = futureCall(new StrJob<>(), immediate(Long.valueOf(123)));
      PipelineService service = PipelineServiceFactory.newPipelineService();
      String str1 = service.startNewPipeline(new EchoJob<>(), child2);
      String str2 = service.startNewPipeline(new CalledJob(), waitFor(child1));
      return immediate(str1 + "," + str2);
    }
  }

  @SuppressWarnings("serial")
  private static class EchoJob<T> extends Job1<T, T> {

    @Override
    public Value<T> run(T t) throws Exception {
      return immediate(t);
    }
  }

  @SuppressWarnings("serial")
  private static class ChildJob extends Job0<Void> {

    @Override
    public Value<Void> run() throws Exception {
      Thread.sleep(5000);
      CallerJob.flag.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class CalledJob extends Job0<Boolean> {

    @Override
    public Value<Boolean> run() throws Exception {
      return immediate(CallerJob.flag.get());
    }
  }

  public void testWaitForUsedByNewPipeline() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new CallerJob());
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    String[] calledPipelines = ((String) jobInfo.getOutput()).split(",");
    jobInfo = waitUntilJobComplete(calledPipelines[0]);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertEquals("123", jobInfo.getOutput());
    jobInfo = waitUntilJobComplete(calledPipelines[1]);
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
    assertTrue((Boolean) jobInfo.getOutput());
  }


  @SuppressWarnings("serial")
  private abstract static class AbstractJob extends Job0<String> {

    @Override
    public Value<String> run() throws Exception {
      return immediate(getValue());
    }

    protected abstract String getValue();
  }

  @SuppressWarnings("serial")
  private static class ConcreteJob extends AbstractJob {

    @Override
    protected String getValue() {
      return "Shalom";
    }

    @Override
    public String getJobDisplayName() {
      return "ConcreteJob: " + getValue();
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(Throwable t) {
      return immediate("Got exception!");
    }
  }

  public void testGetJobDisplayName() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    ConcreteJob job = new ConcreteJob();
    String pipelineId = service.startNewPipeline(job);
    JobRecord jobRecord = PipelineManager.getJob(pipelineId);
    assertEquals(job.getJobDisplayName(), jobRecord.getRootJobDisplayName());
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    assertEquals("Shalom", jobInfo.getOutput());
    jobRecord = PipelineManager.getJob(pipelineId);
    assertEquals(job.getJobDisplayName(), jobRecord.getRootJobDisplayName());
    PipelineObjects pobjects = PipelineManager.queryFullPipeline(pipelineId);
    assertEquals(job.getJobDisplayName(), pobjects.rootJob.getRootJobDisplayName());
  }

  public void testJobInheritence() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new ConcreteJob());
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    assertEquals("Shalom", jobInfo.getOutput());
  }

  @SuppressWarnings("serial")
  private static class FailedJob extends Job0<String> {

    @Override
    public Value<String> run() throws Exception {
      throw new RuntimeException("koko");
    }
  }

 public void testJobFailure() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new FailedJob());
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    assertEquals(JobInfo.State.STOPPED_BY_ERROR, jobInfo.getJobState());
    assertEquals("koko", jobInfo.getException().getMessage());
    assertNull(jobInfo.getOutput());
  }

  public void testReturnValue() throws Exception {
    // Testing that return value from parent is always after all children complete
    // which is not the case right now. This this *SHOULD* change after we fix
    // it, as fixing it should cause a dead-lock.
    // see b/12249138
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new ReturnValueParentJob());
    String value = waitForJobToComplete(pipelineId);
    assertEquals("bla", value);
    ReturnValueParentJob.latch1.countDown();
    waitUntilTaskQueueIsEmpty();
    ReturnValueParentJob.latch2.await();
  }

  @SuppressWarnings("serial")
  private static class ReturnValueParentJob extends Job0<String> {

    static CountDownLatch latch1 = new CountDownLatch(1);
    static CountDownLatch latch2 = new CountDownLatch(1);

    @Override
    public Value<String> run() throws Exception {
      futureCall(new ReturnedValueChildJob());
      return immediate("bla");
    }
  }

  @SuppressWarnings("serial")
  private static class ReturnedValueChildJob extends Job0<Void> {
    @Override
    public Value<Void> run() throws Exception {
      ReturnValueParentJob.latch1.await();
      ReturnValueParentJob.latch2.countDown();
      return null;
    }
  }

  public void testSubmittingPromisedValueMoreThanOnce() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new SubmitPromisedParentJob());
    String value = waitForJobToComplete(pipelineId);
    assertEquals("2", value);
  }

  @SuppressWarnings("serial")
  private static class SubmitPromisedParentJob extends Job0<String> {

    @Override
    public Value<String> run() throws Exception {
      PromisedValue<String> promise = newPromise();
      FutureValue<Void> child1 = futureCall(
          new FillPromiseJob(), immediate("1"), immediate(promise.getHandle()));
      FutureValue<Void> child2 = futureCall(
          new FillPromiseJob(), immediate("2"), immediate(promise.getHandle()), waitFor(child1));
      FutureValue<String> child3 = futureCall(new ReadPromiseJob(), promise, waitFor(child2));
      // If we return promise directly then the value would be "1" rather than "2", see b/12216307
      //return promise;
      return child3;
    }
  }

  @SuppressWarnings("serial")
  private static class ReadPromiseJob extends Job1<String, String> {
    @Override
    public Value<String> run(String value) {
      return immediate(value);
    }
  }

  @SuppressWarnings("serial")
  private static class FillPromiseJob extends Job2<Void, String, String> {

    @Override
    public Value<Void> run(String value, String handle) throws Exception {
      PipelineServiceFactory.newPipelineService().submitPromisedValue(handle, value);
      return null;
    }
  }

  public void testCancelPipeline() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new HandleExceptionParentJob(),
        new JobSetting.BackoffSeconds(1), new JobSetting.BackoffFactor(1),
        new JobSetting.MaxAttempts(2));
    JobInfo jobInfo = service.getJobInfo(pipelineId);
    assertEquals(State.RUNNING, jobInfo.getJobState());
    HandleExceptionChild2Job.childLatch1.await();
    service.cancelPipeline(pipelineId);
    HandleExceptionChild2Job.childLatch2.countDown();
    waitUntilTaskQueueIsEmpty();
    jobInfo = service.getJobInfo(pipelineId);
    assertEquals(State.CANCELED_BY_REQUEST, jobInfo.getJobState());
    assertNull(jobInfo.getOutput());
    assertTrue(HandleExceptionParentJob.child0.get());
    assertTrue(HandleExceptionParentJob.child1.get());
    assertTrue(HandleExceptionParentJob.child2.get());
    assertFalse(HandleExceptionParentJob.child3.get());
    assertFalse(HandleExceptionParentJob.child4.get());
    // Unexpected callbacks (should be fixed after b/12250957)

    assertTrue(HandleExceptionParentJob.child3Cancel.get()); // job not started
    assertTrue(HandleExceptionParentJob.child4Cancel.get()); // job not started

    // expected callbacks
    assertFalse(HandleExceptionParentJob.child0Cancel.get()); // job already finalized
    assertTrue(HandleExceptionParentJob.parentCancel.get());
    assertTrue(HandleExceptionParentJob.child1Cancel.get());
    assertTrue(HandleExceptionParentJob.child2Cancel.get()); // after job run, but not finalized
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionParentJob extends Job0<String> {

    private static AtomicBoolean parentCancel = new AtomicBoolean();
    private static AtomicBoolean child0 = new AtomicBoolean();
    private static AtomicBoolean child0Cancel = new AtomicBoolean();
    private static AtomicBoolean child1 = new AtomicBoolean();
    private static AtomicBoolean child1Cancel = new AtomicBoolean();
    private static AtomicBoolean child2 = new AtomicBoolean();
    private static AtomicBoolean child2Cancel = new AtomicBoolean();
    private static AtomicBoolean child3 = new AtomicBoolean();
    private static AtomicBoolean child3Cancel = new AtomicBoolean();
    private static AtomicBoolean child4 = new AtomicBoolean();
    private static AtomicBoolean child4Cancel = new AtomicBoolean();

    @Override
    public Value<String> run() {
      FutureValue<String> child0 = futureCall(new HandleExceptionChild0Job());
      FutureValue<String> child1 = futureCall(new HandleExceptionChild1Job(), waitFor(child0));
      FutureValue<String> child2 = futureCall(new HandleExceptionChild3Job(), waitFor(child1));
      return futureCall(new HandleExceptionChild4Job(), child1, child2);
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) throws Exception {
      HandleExceptionParentJob.parentCancel.set(true);
      return immediate("should not be used");
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild0Job extends Job0<String> {
    @Override
    public Value<String> run() {
      HandleExceptionParentJob.child0.set(true);
      return immediate("1");
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child0Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild1Job extends Job0<String> {

    @Override
    public Value<String> run() {
      HandleExceptionParentJob.child1.set(true);
      return futureCall(new HandleExceptionChild2Job());
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child1Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild2Job extends Job0<String> {

    private static CountDownLatch childLatch1 = new CountDownLatch(1);
    private static CountDownLatch childLatch2 = new CountDownLatch(1);

    @Override
    public Value<String> run() throws InterruptedException {
      HandleExceptionParentJob.child2.set(true);
      childLatch1.countDown();
      childLatch2.await();
      Thread.sleep(1000);
      return immediate("1");
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child2Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild3Job extends Job0<String> {

    @Override
    public Value<String> run() {
      HandleExceptionParentJob.child3.set(true);
      return immediate("2");
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child3Cancel.set(true);
      return null;
    }
  }

  @SuppressWarnings("serial")
  private static class HandleExceptionChild4Job extends Job2<String, String, String> {

    @Override
    public Value<String> run(String str1, String str2) {
      HandleExceptionParentJob.child4.set(true);
      return immediate(str1 + str2);
    }

    @SuppressWarnings("unused")
    public Value<String> handleException(CancellationException ex) {
      HandleExceptionParentJob.child4Cancel.set(true);
      return immediate("not going to be used");
    }
  }

  public void testImmediateChild() throws Exception {
    // This is also testing inheritance of statusConsoleUrl.
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new Returns5FromChildJob(), largeValue);
    Integer five = waitForJobToComplete(pipelineId);
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

    private final T value;

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
