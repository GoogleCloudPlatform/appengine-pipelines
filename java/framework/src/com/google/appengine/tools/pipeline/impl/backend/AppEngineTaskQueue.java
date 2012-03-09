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

package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Encapsulates access to the App Engine Task Queue API
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class AppEngineTaskQueue implements PipelineTaskQueue {

  private static final Logger logger = Logger.getLogger(AppEngineTaskQueue.class.getName());

  private static final int MAX_TASKS_PER_ENQUEUE = 100;

  // TODO(ohler): make this a parameter
  private final Queue taskQueue = QueueFactory.getDefaultQueue();

  @Override
  public void enqueue(Task task) {
    logger.finest("Enqueueing: " + task);
    taskQueue.add(toTaskOptions(task));
  }

  @Override
  public void enqueue(final Collection<Task> tasks) {
    List<TaskOptions> taskOptionsList = new LinkedList<TaskOptions>();
    for (Task task : tasks) {
      logger.finest("Enqueueing: " + task);
      taskOptionsList.add(toTaskOptions(task));
      if (taskOptionsList.size() >= MAX_TASKS_PER_ENQUEUE) {
        taskQueue.add(taskOptionsList);
        taskOptionsList = new LinkedList<TaskOptions>();
      }
    }
    if (taskOptionsList.size() > 0) {
      taskQueue.add(taskOptionsList);
    }
  }

  private TaskOptions toTaskOptions(Task task) {
    TaskOptions taskOptions = TaskOptions.Builder.withUrl(TaskHandler.HANDLE_TASK_URL);
    if (task.getOnBackend() != null) {
      taskOptions.header("Host",
          BackendServiceFactory.getBackendService().getBackendAddress(task.getOnBackend()));
    }
    addProperties(taskOptions, task.toProperties());
    String taskName = task.getName();
    if (null != taskName) {
      taskOptions.taskName(taskName);
    }
    Long delaySeconds = task.getDelaySeconds();
    if (null != delaySeconds) {
      taskOptions.countdownMillis(delaySeconds * 1000L);
    }
    return taskOptions;
  }

  @SuppressWarnings("unchecked")
  private static void addProperties(TaskOptions taskOptions, Properties properties) {
    Enumeration<String> paramNames = (Enumeration<String>) properties.propertyNames();
    while (paramNames.hasMoreElements()) {
      String paramName = paramNames.nextElement();
      String paramValue = properties.getProperty(paramName);
      taskOptions.param(paramName, paramValue);
    }
  }

}
