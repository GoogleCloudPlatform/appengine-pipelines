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
import javax.servlet.http.HttpServletResponse;

/**
 * @author rudominer@google.com (Your Name Here)
 * 
 */
public class TaskHandler {

  private static Logger logger = Logger.getLogger(TaskHandler.class.getName());

  public static final String PATH_COMPONENT = "handleTask";
  public static final String HANDLE_TASK_URL = PipelineServlet.BASE_URL + PATH_COMPONENT;

  public static final String TASK_NAME_REQUEST_HEADER = "X-AppEngine-TaskName";
  public static final String TASK_RETRY_COUNT_HEADER = "X-AppEngine-TaskRetryCount";

  public static void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException {
    Task task = null;
    int retryCount = -1;
    try {
      retryCount = Integer.parseInt(req.getHeader(TASK_RETRY_COUNT_HEADER));
    } catch (Exception e) {
      // ignore
    }
    try {
      task = reconstructTask(req);
      PipelineManager.processTask(task);
    } catch (Exception e) {
      StringUtils.logRetryMessage(logger, task, retryCount, e);
      throw new ServletException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Task reconstructTask(HttpServletRequest request) {
    Properties properties = new Properties();
    Enumeration paramNames = request.getParameterNames();
    while (paramNames.hasMoreElements()) {
      String paramName = (String) paramNames.nextElement();
      String paramValue = request.getParameter(paramName);
      properties.setProperty(paramName, paramValue);
    }
    Task task = Task.fromProperties(properties);
    String taskName = request.getHeader(TASK_NAME_REQUEST_HEADER);
    task.setName(taskName);
    return task;
  }
}
