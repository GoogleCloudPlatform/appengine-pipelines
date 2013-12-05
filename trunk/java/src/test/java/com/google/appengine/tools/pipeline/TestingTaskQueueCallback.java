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

import com.google.appengine.api.taskqueue.dev.LocalTaskQueueCallback;
import com.google.appengine.api.urlfetch.URLFetchServicePb;
import com.google.appengine.api.urlfetch.URLFetchServicePb.URLFetchRequest;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;

import java.net.URLDecoder;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A {@code LocalTaskQueueCallback} for use in tests that make use of the
 * Pipeline framework.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@SuppressWarnings("serial")
public class TestingTaskQueueCallback implements LocalTaskQueueCallback {
  Logger logger = Logger.getLogger(TestingTaskQueueCallback.class.getName());

  /**
   * Execute the provided url fetch request.
   *
   * @param req The url fetch request
   * @return The HTTP status code of the fetch.
   */
  @Override
  public int execute(URLFetchServicePb.URLFetchRequest req) {
    String taskName = null;
    int retryCount = -1;
    String queueName = null;
    for (URLFetchRequest.Header pbHeader : req.getHeaderList()) {
      String headerName = pbHeader.getKey();
      String headerValue = pbHeader.getValue();
      if (TaskHandler.TASK_NAME_REQUEST_HEADER.equalsIgnoreCase(headerName)) {
        taskName = headerValue;
      } else if (TaskHandler.TASK_RETRY_COUNT_HEADER.equalsIgnoreCase(headerName)) {
        try {
          retryCount = Integer.parseInt(headerValue);
        } catch (Exception e) {
          // ignore
        }
      } else if (TaskHandler.TASK_QUEUE_NAME_HEADER.equalsIgnoreCase(headerName)) {
        queueName = headerValue;
      }
    }
    String requestBody = req.getPayload().toStringUtf8();
    String[] params = requestBody.split("&");
    Properties properties = new Properties();
    Task task = null;
    try {
      for (String param : params) {
        String[] pair = param.split("=");
        String name = pair[0];
        String value = pair[1];
        String decodedValue = URLDecoder.decode(value, "UTF8");
        properties.put(name, decodedValue);
      }
      task = Task.fromProperties(taskName, properties);
      if (queueName != null && task.getQueueSettings().getOnQueue() == null) {
        task.getQueueSettings().setOnQueue(queueName);
      }
      PipelineManager.processTask(task);

    } catch (Exception e) {
      StringUtils.logRetryMessage(logger, task, retryCount, e);
      return 500;
    }
    return 200;
  }

  @Override
  public void initialize(Map<String, String> arg0) {
  }
}
