package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

public class AppEngineTaskQueue implements CascadeTaskQueue {

  private static final Logger logger = Logger.getLogger(AppEngineTaskQueue.class.getName());

  private Queue taskQueue;

  {
    taskQueue = QueueFactory.getDefaultQueue();
  }

  public void enqueue(Task task) {
    logger.finest("Enqueueing: "+task);
    taskQueue.add(toTaskOptions(task));
  }

  private TaskOptions toTaskOptions(Task task) {
    TaskOptions taskOptions = TaskOptions.Builder.withUrl(TaskHandler.HANDLE_TASK_URL);
    addProperties(taskOptions, task.toProperties());
    String taskName = task.getName();
    if (null != taskName){
      taskOptions.taskName(taskName);
    }
    Long delaySeconds = task.getDelaySeconds();
    if (null != delaySeconds){
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
