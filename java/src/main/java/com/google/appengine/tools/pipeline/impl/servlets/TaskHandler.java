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

package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;

import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

/**
 * A ServletHelper that handles all requests from the task queue.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class TaskHandler {

  private static Logger logger = Logger.getLogger(TaskHandler.class.getName());

  public static final String PATH_COMPONENT = "handleTask";
  public static final String HANDLE_TASK_URL = PipelineServlet.BASE_URL + PATH_COMPONENT;

  public static final String TASK_NAME_REQUEST_HEADER = "X-AppEngine-TaskName";
  public static final String TASK_RETRY_COUNT_HEADER = "X-AppEngine-TaskRetryCount";
  public static final String TASK_QUEUE_NAME_HEADER = "X-AppEngine-QueueName";

  public static void doPost(HttpServletRequest req) throws ServletException {
    Task task = reconstructTask(req);
    int retryCount;
    try {
      retryCount = req.getIntHeader(TASK_RETRY_COUNT_HEADER);
    } catch (NumberFormatException e) {
      retryCount = -1;
    }
    try {
      PipelineManager.processTask(task);
    } catch (RuntimeException e) {
      StringUtils.logRetryMessage(logger, task, retryCount, e);
      throw new ServletException(e);
    }
  }

  private static Task reconstructTask(HttpServletRequest request) {
    Properties properties = new Properties();
    Enumeration<?> paramNames = request.getParameterNames();
    while (paramNames.hasMoreElements()) {
      String paramName = (String) paramNames.nextElement();
      String paramValue = request.getParameter(paramName);
      properties.setProperty(paramName, paramValue);
    }
    String taskName = request.getHeader(TASK_NAME_REQUEST_HEADER);
    Task task = Task.fromProperties(taskName, properties);
    String queueName = request.getHeader(TASK_QUEUE_NAME_HEADER);
    if (queueName != null && task.getQueueSettings().getOnQueue() == null) {
      task.getQueueSettings().setOnQueue(queueName);
    }
    return task;
  }
}
