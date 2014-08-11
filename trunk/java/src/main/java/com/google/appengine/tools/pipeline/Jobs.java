// Copyright 2014 Google Inc.
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

import static com.google.appengine.tools.pipeline.Job.immediate;
import static com.google.appengine.tools.pipeline.Job.waitFor;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.DeferredTaskContext;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

/**
 * A collection of common jobs and utilities.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public class Jobs {

  private Jobs() {
    // A utility class
  }


  /**
   * An helper job to transform a {@link Value} result.
   * @param <F> input value.
   * @param <T> transformed value.
   */
  public static class Transform<F, T> extends Job1<T, F> {

    private static final long serialVersionUID = 1280795955105207728L;
    private Function<F, T> function;

    public Transform(Function<F, T> function) {
      Preconditions.checkArgument(function instanceof Serializable, "Function not serializable");
      this.function = function;
    }

    @Override
    public Value<T> run(F from) throws Exception {
      return immediate(function.apply(from));
    }
  }

  private static class WaitForAllJob<T> extends Job1<T, T> {

    private static final long serialVersionUID = 3151677893523195265L;

    @Override
    public Value<T> run(T value) {
      return immediate(value);
    }
  }

  public static JobSetting.WaitForSetting[] createWaitForSettingArray(Value<?>... values) {
    JobSetting.WaitForSetting[] settings = new JobSetting.WaitForSetting[values.length];
    int i = 0;
    for (Value<?> value : values) {
      settings[i++] = waitFor(value);
    }
    return settings;
  }

  public static <T> Value<T> waitForAll(Job<?> caller, Value<T> value, Value<?>... values) {
    return caller.futureCall(new WaitForAllJob<T>(), value, createWaitForSettingArray(values));
  }

  public static <T> Value<T> waitForAll(Job<?> caller, T value, Value<?>... values) {
    return caller.futureCall(new WaitForAllJob<T>(), immediate(value),
        createWaitForSettingArray(values));
  }

  public static <T> Value<T> waitForAllAndDelete(
      Job<?> caller, Value<T> value, Value<?>... values) {
    return caller.futureCall(
        new DeletePipelineJob<T>(caller.getPipelineKey().getName()),
        value, createWaitForSettingArray(values));
  }

  public static <T> Value<T> waitForAllAndDelete(Job<?> caller, T value, Value<?>... values) {
    return caller.futureCall(new DeletePipelineJob<T>(caller.getPipelineKey().getName()),
        immediate(value), createWaitForSettingArray(values));
  }

  private static class DeletePipelineJob<T> extends Job1<T, T> {

    private static final long serialVersionUID = -5440838671291502355L;
    private static final Logger log = Logger.getLogger(DeletePipelineJob.class.getName());
    private final String key;

    DeletePipelineJob(String rootJobKey) {
      this.key = rootJobKey;
    }

    @Override
    public Value<T> run(T value) {
      DeferredTask deleteRecordsTask = new DeferredTask() {
        private static final long serialVersionUID = -7510918963650055768L;

        @Override
        public void run() {
          PipelineService service = PipelineServiceFactory.newPipelineService();
          try {
            service.deletePipelineRecords(key);
            log.info("Deleted pipeline: " + key);
          } catch (IllegalStateException e) {
            log.info("Failed to delete pipeline: " + key);
            HttpServletRequest request = DeferredTaskContext.getCurrentRequest();
            if (request != null) {
              int attempts = request.getIntHeader("X-AppEngine-TaskExecutionCount");
              if (attempts <= 5) {
                log.info("Request to retry deferred task #" + attempts);
                DeferredTaskContext.markForRetry();
                return;
              }
            }
            try {
              service.deletePipelineRecords(key, true, false);
              log.info("Force deleted pipeline: " + key);
            } catch (Exception ex) {
              log.log(Level.WARNING, "Failed to force delete pipeline: " + key, ex);
            }
          } catch (NoSuchObjectException e) {
            // Already done
          }
        }
      };
      String queueName = Optional.fromNullable(getOnQueue()).or("default");
      Queue queue = QueueFactory.getQueue(queueName);
      queue.add(TaskOptions.Builder.withPayload(deleteRecordsTask).countdownMillis(10000)
          .retryOptions(RetryOptions.Builder.withMinBackoffSeconds(2).maxBackoffSeconds(20)));
      return immediate(value);
    }
  }
}
