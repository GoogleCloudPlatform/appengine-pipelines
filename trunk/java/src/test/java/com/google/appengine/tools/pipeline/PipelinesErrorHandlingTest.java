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

import com.google.appengine.tools.pipeline.impl.PipelineManager;

import java.nio.channels.AlreadyConnectedException;
import java.util.concurrent.CancellationException;

/**
 * Test error handling through handleException.
 *
 * @author maximf@google.com (Maxim Fateev)
 */
public class PipelinesErrorHandlingTest extends PipelineTest {

  private static final int EXPECTED_RESULT1 = 5;
  private static final int EXPECTED_RESULT2 = 522;
  private static final int EXPECTED_RESULT3 = 223;

  private PipelineService service = PipelineServiceFactory.newPipelineService();

  @SuppressWarnings("serial")
  static class TestImmediateThrowCatchJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestImmediateThrowCatchJob.run");
      return futureCall(new ImmediateThrowCatchJob(), new JobSetting.MaxAttempts(1));
    }
  }

  @SuppressWarnings("serial")
  static class ImmediateThrowCatchJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("ImmediateThrowCatchJob.run");
      throw new IllegalStateException("simulated");
    }

    // Unrelated to IllegalStateException
    public Value<Integer> handleException(NullPointerException e) {
      assertNotNull(e);
      trace("ImmediateThrowCatchJob.handleException.NullPointerException");
      return immediate(EXPECTED_RESULT1);
    }

    public Value<Integer> handleException(IllegalStateException e) {
      assertNotNull(e);
      trace("ImmediateThrowCatchJob.handleException.IllegalStateException");
      return immediate(EXPECTED_RESULT1);
    }

    // Subclass of IllegalStateException
    public Value<Integer> handleException(AlreadyConnectedException e) {
      assertNotNull(e);
      trace("ImmediateThrowCatchJob.handleException.AlreadyBoundException");
      return immediate(EXPECTED_RESULT1);
    }

    public Value<Integer> handleException(Throwable e) {
      assertNotNull(e);
      trace("ImmediateThrowCatchJob.handleException.Throwable");
      return immediate(EXPECTED_RESULT1);
    }
  }

  public void testImmediateThrowCatchJob() throws Exception {
    String pipelineId = service.startNewPipeline(new TestImmediateThrowCatchJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());
    assertEquals("TestImmediateThrowCatchJob.run ImmediateThrowCatchJob.run "
        + "ImmediateThrowCatchJob.handleException.IllegalStateException", trace());
  }

  @SuppressWarnings("serial")
  private static class AngryJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("AngryJob.run");
      throw new IllegalStateException("simulated");
    }
  }

  @SuppressWarnings("serial")
  static class TestSimpleCatchJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestSimpleCatchJob.run");
      return futureCall(
          new AngryJob(), new JobSetting.MaxAttempts(2), new JobSetting.BackoffSeconds(1));
    }

    // Unrelated to IllegalStateException
    public Value<Integer> handleException(NullPointerException e) {
      assertNotNull(e);
      trace("TestSimpleCatchJob.handleException.NullPointerException");
      return immediate(EXPECTED_RESULT1);
    }

    public Value<Integer> handleException(IllegalStateException e) {
      assertNotNull(e);
      trace("TestSimpleCatchJob.handleException.IllegalStateException");
      return immediate(EXPECTED_RESULT1);
    }

    // Subclass of IllegalStateException
    public Value<Integer> handleException(AlreadyConnectedException e) {
      assertNotNull(e);
      trace("TestSimpleCatchJob.handleException.AlreadyBoundException");
      return immediate(EXPECTED_RESULT1);
    }

    public Value<Integer> handleException(Throwable e) {
      assertNotNull(e);
      trace("TestSimpleCatchJob.handleException.Throwable");
      return immediate(EXPECTED_RESULT1);
    }
  }

  public void testSimpleCatch() throws Exception {
    String pipelineId = service.startNewPipeline(new TestSimpleCatchJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());
    assertEquals("TestSimpleCatchJob.run AngryJob.run AngryJob.run "
        + "TestSimpleCatchJob.handleException.IllegalStateException", trace());
  }

  @SuppressWarnings("serial")
  static class TestCatchWithImmediateReturnJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() throws Exception {
      FutureValue<Integer> result = futureCall(new CatchWithImmediateReturnJob());
      return futureCall(new CheckResultJob(), result, waitFor(newDelayedValue(3)));
    }
  }

  @SuppressWarnings("serial")
  static class CheckResultJob extends Job1<Integer, Integer> {

    @Override
    public Value<Integer> run(Integer toCheck) throws Exception {
      assertEquals(EXPECTED_RESULT2, toCheck.intValue());
      return immediate(toCheck);
    }
  }

  @SuppressWarnings("serial")
  static class CatchWithImmediateReturnJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestSimpleCatchJob.run");
      futureCall(new AngryJob(), new JobSetting.MaxAttempts(1));
      return immediate(EXPECTED_RESULT1);
    }

    public Value<Integer> handleException(IllegalStateException e) {
      assertNotNull(e);
      trace("TestSimpleCatchJob.handleException.IllegalStateException");
      return immediate(EXPECTED_RESULT2);
    }
  }

  /**
   * Test that result of the run method is always overridden by the result of
   * catch
   */
  public void testCatchWithImmediateReturnJob() throws Exception {
    String pipelineId = service.startNewPipeline(new TestCatchWithImmediateReturnJob());
    waitForJobToComplete(pipelineId);
    assertEquals("TestSimpleCatchJob.run AngryJob.run "
        + "TestSimpleCatchJob.handleException.IllegalStateException", trace());
  }

  @SuppressWarnings("serial")
  private static class AngryJobWithRethrowingFailureHandler extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("AngryJob.run");
      throw new IllegalStateException("simulated");
    }

    @SuppressWarnings("unused")
    public Value<Integer> handleException(Throwable e) throws Throwable {
      trace("AngryJob.handleException");
      throw e;
    }
  }

  @SuppressWarnings("serial")
  static class TestCatchRethrowingJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestSimpleCatchJob.run");
      return futureCall(new AngryJobWithRethrowingFailureHandler(), new JobSetting.MaxAttempts(2),
          new JobSetting.BackoffSeconds(1));
    }

    public Value<Integer> handleException(IllegalStateException e) throws Throwable {
      assertNotNull(e);
      trace("TestSimpleCatchJob.handleException");
      return immediate(EXPECTED_RESULT1);
    }
  }

  public void testCatchRethrowing() throws Exception {
    String pipelineId = service.startNewPipeline(new TestCatchRethrowingJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());
    assertEquals("TestSimpleCatchJob.run AngryJob.run AngryJob.run AngryJob.handleException "
        + "TestSimpleCatchJob.handleException", trace());
  }

  @SuppressWarnings("serial")
  static class TestCatchGeneratorJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      return futureCall(new AngryJob(), new JobSetting.MaxAttempts(1));
    }

    public Value<Integer> handleException(Throwable e) throws Throwable {
      return futureCall(new FailureHandlingJob(), immediate(e));
    }
  }

  @SuppressWarnings("serial")
  static class FailureHandlingJob extends Job1<Integer, Throwable> {

    @Override
    public Value<Integer> run(Throwable e) {
      assertNotNull(e);
      assertEquals(IllegalStateException.class, e.getClass());
      return immediate(EXPECTED_RESULT1);
    }
  }

  public void testCatchGenerator() throws Exception {
    String pipelineId = service.startNewPipeline(new TestCatchGeneratorJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());
  }

  @SuppressWarnings("serial")
  static class TestChildThrowingJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      return futureCall(new AngryChildJob(), new JobSetting.MaxAttempts(1));
    }

    public Value<Integer> handleException(IllegalStateException e) {
      assertNotNull(e);
      return immediate(EXPECTED_RESULT1);
    }
  }

  @SuppressWarnings("serial")
  static class AngryChildJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      return futureCall(new AngryJob(), new JobSetting.MaxAttempts(1));
    }
  }

  public void testChildThrowing() throws Exception {
    String pipelineId = service.startNewPipeline(new TestChildThrowingJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());
  }

  @SuppressWarnings("serial")
  static class TestChildCancellationJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestChildCancellationJob.run");
      return futureCall(new ParentOfAngryChildJob(), new JobSetting.MaxAttempts(1));
    }

    public Value<Integer> handleException(IllegalStateException e) {
      trace("TestChildCancellationJob.handleException");
      assertNotNull(e);
      return immediate(EXPECTED_RESULT1);
    }
  }

  @SuppressWarnings("serial")
  static class JobToCancel extends Job1<Integer, Integer> {

    @Override
    public Value<Integer> run(Integer param1) {
      trace("JobToCancel.run");
      throw new IllegalStateException("should not execute");
    }

    public Value<Integer> handleException(CancellationException e) throws Throwable {
      trace("JobToCancel.handleException");
      assertNotNull(e);
      return immediate(EXPECTED_RESULT1);
    }
  }

  @SuppressWarnings("serial")
  static class ParentOfAngryChildJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("ParentOfAngryChildJob.run");
      // firstChild should never execute as it waits on the promise that is
      // never ready
      PromisedValue<Integer> neverReady = newPromise();
      FutureValue<Integer> firstChild =
          futureCall(new JobToCancel(), neverReady, new JobSetting.MaxAttempts(1));
      // This one failing should cause cancellation of the first job, which
      // should execute its handleException(CancellationException);
      futureCall(new AngryJob(), new JobSetting.MaxAttempts(1));
      return firstChild;
    }
  }

  public void testChildCancellation() throws Exception {
    String pipelineId = service.startNewPipeline(new TestChildCancellationJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());
    // TODO(user): After implementing handleFinally which requires child
    // reference counting the order of operations will be exactly defined.
    boolean expectedTraceChildCancelledFirst = ("TestChildCancellationJob.run "
        + "ParentOfAngryChildJob.run AngryJob.run "
        + "JobToCancel.handleException TestChildCancellationJob.handleException").equals(trace());
    boolean expectedTraceParentJobHandlerFirst = ("TestChildCancellationJob.run "
        + "ParentOfAngryChildJob.run AngryJob.run "
        + "TestChildCancellationJob.handleException JobToCancel.handleException").equals(trace());
    assertTrue(expectedTraceChildCancelledFirst || expectedTraceParentJobHandlerFirst);
  }

  @SuppressWarnings("serial")
  static class TestGrandchildCancellationJob extends Job0<Void> {

    @Override
    public Value<Void> run() {
      trace("TestGrandchildCancellationJob.run");
      return futureCall(
          new ParentOfGrandchildToCancelAndAngryChildJob(), new JobSetting.MaxAttempts(1));
    }

    public Value<Integer> handleException(IllegalStateException e) {
      trace("TestGrandchildCancellationJob.handleException");
      assertNotNull(e);
      return immediate(EXPECTED_RESULT1);
    }
  }

  @SuppressWarnings("serial")
  static class ParentOfJobToCancel extends Job1<Integer, String> {

    @Override
    public Value<Integer> run(String unblockTheAngryOneHandle) throws Exception {
      trace("ParentOfJobToCancel.run");
      // Unblocks a sibling that is going to throw an exception
      PipelineManager.acceptPromisedValue(unblockTheAngryOneHandle, EXPECTED_RESULT1);
      PromisedValue<Integer> neverReady = newPromise();
      return futureCall(new JobToCancel(), neverReady);
    }
  }

  @SuppressWarnings("serial")
  private static class DelayedAngryJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("DelayedAngryJob.run");
      return futureCall(new AngryJob(), waitFor(newDelayedValue(1)), new JobSetting.MaxAttempts(1));
    }
  }

  @SuppressWarnings("serial")
  static class ParentOfGrandchildToCancelAndAngryChildJob extends Job0<Void> {

    @Override
    public Value<Void> run() {
      trace("ParentOfGrandchildToCancelAndAngryChildJob.run");
      PromisedValue<Integer> unblockTheAngryOne = newPromise();
      futureCall(new ParentOfJobToCancel(), immediate(unblockTheAngryOne.getHandle()),
          new JobSetting.MaxAttempts(1));
      // This one failing should cause cancellation of the first job, which
      // should execute its error handling job (SimpleCatchJob);
      futureCall(new DelayedAngryJob(), waitFor(unblockTheAngryOne), new JobSetting.MaxAttempts(1));
      return newDelayedValue(10);
    }
  }

  /**
   * Test cancellation of a child of a generator job that had a failed sibling.
   */
  public void testGrandchildCancellation() throws Exception {
    String pipelineId = service.startNewPipeline(new TestGrandchildCancellationJob());
    waitUntilTaskQueueIsEmpty();
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());

    boolean expectedTraceChildCancelledFirst = ("TestGrandchildCancellationJob.run "
        + "ParentOfGrandchildToCancelAndAngryChildJob.run ParentOfJobToCancel.run"
        + "DelayedAngryJob.run AngryJob.run "
        + "JobToCancel.handleException TestGrandchildCancellationJob.handleException").equals(
        trace());
    boolean expectedTraceParentJobHandlerFirst = ("TestGrandchildCancellationJob.run "
        + "ParentOfGrandchildToCancelAndAngryChildJob.run ParentOfJobToCancel.run "
        + "DelayedAngryJob.run AngryJob.run "
        + "TestGrandchildCancellationJob.handleException JobToCancel.handleException").equals(
        trace());
    assertTrue(trace(), expectedTraceChildCancelledFirst || expectedTraceParentJobHandlerFirst);
  }

  @SuppressWarnings("serial")
  static class TestChildCancellationFailingJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestChildCancellationFailingJob.run");
      return futureCall(new ParentOfAngryChildJobWithJobToCancelThatFailsToCancel(),
          new JobSetting.MaxAttempts(1));
    }

    public Value<Integer> handleException(IllegalStateException e) {
      trace("TestChildCancellationFailingJob.handleException");
      assertNotNull(e);
      return immediate(EXPECTED_RESULT1);
    }
  }

  @SuppressWarnings("serial")
  static class JobToCancelThatFailsToCancel extends Job1<Integer, Integer> {

    @Override
    public Value<Integer> run(Integer param1) {
      trace("JobToCancelThatFailsToCancel.run");
      throw new IllegalStateException("should not execute");
    }

    public Value<Integer> handleException(CancellationException e) throws Throwable {
      trace("JobToCancelThatFailsToCancel.handleException");
      assertNotNull(e);
      // this exception is ignored
      throw new IllegalArgumentException("simulated throw from cancel");
    }
  }

  @SuppressWarnings("serial")
  static class ParentOfAngryChildJobWithJobToCancelThatFailsToCancel extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("ParentOfAngryChildJobWithJobToCancelThatFailsToCancel.run");
      // firstChild should never execute as it waits on the promise that is
      // never ready
      PromisedValue<Integer> neverReady = newPromise();
      FutureValue<Integer> firstChild =
          futureCall(new JobToCancelThatFailsToCancel(), neverReady, new JobSetting.MaxAttempts(1));
      // This one failing should cause cancellation of the first job, which
      // should execute its error handling job (SimpleCatchJob);
      futureCall(new AngryJob(), new JobSetting.MaxAttempts(1));
      return firstChild;
    }
  }

  public void testChildCancellationFailure() throws Exception {
    String pipelineId = service.startNewPipeline(new TestChildCancellationFailingJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals(EXPECTED_RESULT1, result.intValue());
    // TODO(user): After implementing handleFinally which requires child
    // reference counting the order of operations will be exactly defined.
    boolean expectedTraceChildCancelledFirst =
        ("TestChildCancellationFailingJob.run "
            + "ParentOfAngryChildJobWithJobToCancelThatFailsToCancel.run AngryJob.run "
            + "JobToCancelThatFailsToCancel.handleException " +
            "TestChildCancellationFailingJob.handleException")
            .equals(trace());
    boolean expectedTraceParentJobHandlerFirst =
        ("TestChildCancellationFailingJob.run "
            + "ParentOfAngryChildJobWithJobToCancelThatFailsToCancel.run AngryJob.run "
            + "TestChildCancellationFailingJob.handleException " +
            "JobToCancelThatFailsToCancel.handleException")
            .equals(trace());
    assertTrue(trace(), expectedTraceChildCancelledFirst || expectedTraceParentJobHandlerFirst);
  }

  @SuppressWarnings("serial")
  static class TestPipelineCancellationJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestPipelineCancellationJob.run");
      return futureCall(new JobToCancelWithFailureHandler(), immediate(10));
    }
  }

  static int catchCount;

  @SuppressWarnings("serial")
  static class JobToCancelWithFailureHandler extends Job1<Integer, Integer> {

    @Override
    public Value<Integer> run(Integer param1) {
      trace("JobToCancelWithFailureHandler.run");
      PromisedValue<Integer> neverReady = newPromise();
      return futureCall(new JobToCancelWithFailureHandler(), neverReady);
    }

    public Value<Integer> handleException(CancellationException e) throws Throwable {
      trace("JobToCancelWithFailureHandler.handleException");
      assertNotNull(e);
      catchCount++;
      return immediate(EXPECTED_RESULT1);
    }
  }

  public void testPipelineCancellation() throws Exception {
    String pipelineId = service.startNewPipeline(new TestPipelineCancellationJob());
    Thread.sleep(2000);
    service.cancelPipeline(pipelineId);
    try {
      waitForJobToComplete(pipelineId);
      fail("should throw");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("canceled by request"));
    }
    assertEquals(2, catchCount);
    assertEquals(
        "TestPipelineCancellationJob.run JobToCancelWithFailureHandler.run "
            + "JobToCancelWithFailureHandler.handleException " +
            "JobToCancelWithFailureHandler.handleException",
        trace());
  }

  @SuppressWarnings("serial")
  static class AngryJobParent extends Job0<Integer> {

    @Override
    public Value<Integer> run() throws Exception {
      trace("AngryJobParent.run");
      return futureCall(new AngryJob(), new JobSetting.MaxAttempts(1));
    }
  }

  /**
   * Validate that that old style "stop pipeline on any error" error handling is
   * used in absence of appropriate exceptionHandler.
   */
  public void testPipelineFailureWhenNoErrorHandlerPresent() {
    String pipelineId = service.startNewPipeline(new AngryJobParent());
    try {
      waitForJobToComplete(pipelineId);
      fail("should throw");
    } catch (Exception e) {
      assertTrue(
          e.getMessage().startsWith("Job stopped java.lang.IllegalStateException: simulated"));
    }
    assertEquals("AngryJobParent.run AngryJob.run", trace());
  }


  @SuppressWarnings("serial")
  static class JobToGetCancellationInHandleException extends Job1<Integer, String> {

    @Override
    public Value<Integer> run(String unblockTheAngryOneHandle) throws Exception {
      trace("JobToGetCancellationInHandleException.run");
      // Unblocks a sibling that is going to throw an exception
      PipelineManager.acceptPromisedValue(unblockTheAngryOneHandle, EXPECTED_RESULT1);
      throw new IllegalStateException("simulated");
    }

    @SuppressWarnings("unused")
    public Value<Integer> handleException(IllegalStateException e)  {
      trace("JobToGetCancellationInHandleException.handleException");
      return futureCall(new CleanupJob());
    }
  }

  /**
   * Invoked from handleException to test long running cleanup.
   */
  @SuppressWarnings("serial")
  static class CleanupJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() throws Exception {
      trace("CleanupJob.run");
      // Use delay to make sure that cleanup takes enough time for cancellation
      // request to arrive
      return futureCall(
          new PassThroughJob1<Integer>(), immediate(EXPECTED_RESULT3), waitFor(newDelayedValue(2)));
      // return immediate(EXPECTED_RESULT);
    }

    @SuppressWarnings("unused")
    public Value<Integer> handleException(Throwable e) {
      // should not be called as parent's handleException is not cancelable.
      // Not using assertion as control flow is checked through trace
      trace("CleanupJob.handleException");
      return immediate(EXPECTED_RESULT2);
    }
  }

  /**
   * Test cancellation of a job that is currently executing handleException
   * descendant.
   */
  @SuppressWarnings("serial")
  static class TestCancellationOfHandleExceptionJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestCancellationOfHandleExceptionJob.run");
      PromisedValue<Integer> unblockTheAngryOne = newPromise();
      futureCall(new JobToGetCancellationInHandleException(),
          immediate(unblockTheAngryOne.getHandle()), new JobSetting.MaxAttempts(1));
      // This one failing should cause cancellation of the first job, which
      // should execute its error handling job (CleanupJob);
      // Delaying for a second to make sure that CleanupJob.run executes
      futureCall(new AngryJob(), waitFor(unblockTheAngryOne), waitFor(newDelayedValue(1)),
          new JobSetting.MaxAttempts(1));
      // Returning promise that is never ready as result of handleException is
      // used
      return newPromise();
    }

    @SuppressWarnings("unused")
    public Value<Integer> handleException(Throwable e) {
      trace("TestCancellationOfHandleExceptionJob.handleException");
      return futureCall(
          new PassThroughJob2<Integer>(), immediate(EXPECTED_RESULT1), waitFor(newDelayedValue(4)));
    }
  }

  @SuppressWarnings("serial")
  static class PassThroughJob1<T> extends Job1<T, T> {

    @Override
    public Value<T> run(T param) throws Exception {
      trace("PassThroughJob1.run");
      return immediate(param);
    }
  }

  @SuppressWarnings("serial")
  static class PassThroughJob2<T> extends Job1<T, T> {

    @Override
    public Value<T> run(T param) throws Exception {
      trace("PassThroughJob2.run");
      return immediate(param);
    }
  }

  /**
   * Test situation when job is cancelled when in handleException
   */
  public void testCancellationOfHandleExceptionJob() throws Exception {
    String pipelineId = service.startNewPipeline(new TestCancellationOfHandleExceptionJob());
    Integer result = waitForJobToComplete(pipelineId);
    assertEquals("TestCancellationOfHandleExceptionJob.run "
        + "JobToGetCancellationInHandleException.run "
        + "JobToGetCancellationInHandleException.handleException CleanupJob.run AngryJob.run "
        + "TestCancellationOfHandleExceptionJob.handleException "
        + "PassThroughJob1.run PassThroughJob2.run", trace());
    assertEquals(EXPECTED_RESULT1, result.intValue());
  }

  @SuppressWarnings("serial")
  static class TestCancellationOfReadyToRunJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestCancellationOfReadyToRunJob.run");
      PromisedValue<Integer> unblockTheSecondOne = newPromise();
      futureCall(new UnblockAndThrowJob(), immediate(unblockTheSecondOne.getHandle()),
          new JobSetting.MaxAttempts(1));

      return futureCall(new PassThroughJob1<Integer>(), unblockTheSecondOne);
    }

    @SuppressWarnings("unused")
    public Value<Integer> handleException(IllegalStateException e) {
      trace("TestCancellationOfReadyToRunJob.handleException");
      return immediate(EXPECTED_RESULT2);
    }
  }

  @SuppressWarnings("serial")
  static class UnblockAndThrowJob extends Job1<Integer, String> {

    @Override
    public Value<Integer> run(String unblockHandle) throws Exception {
      trace("UnblockAndThrowJob.run");
      // TODO(user): uncomment once b/12249138 is fixed, which will be a good test to verify
      // that a job should never return a value before all its children completed and first
      // exception if happens will override possible already filled value.
      // PipelineManager.acceptPromisedValue(unblockHandle, EXPECTED_RESULT1);
      throw new IllegalStateException("simulated");
    }
  }

  /**
   * Test situation when job is cancelled when in handleException
   */
  public void testCancellationOfReadyToRunJob() throws Exception {
    String pipelineId = service.startNewPipeline(new TestCancellationOfReadyToRunJob());
    Integer result = waitForJobToComplete(pipelineId);
    boolean cancellationFirst = ("TestCancellationOfReadyToRunJob.run "
        + "UnblockAndThrowJob.run TestCancellationOfReadyToRunJob.handleException").equals(trace());
    boolean cancelledRunFirst =
        ("TestCancellationOfReadyToRunJob.run "
            + "UnblockAndThrowJob.run PassThroughJob1.run " +
            "TestCancellationOfReadyToRunJob.handleException")
            .equals(trace());

    assertTrue(trace(), cancellationFirst || cancelledRunFirst);
    assertEquals(EXPECTED_RESULT2, result.intValue());
  }
}
