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

package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;

import java.util.Properties;

/**
 * An App Engine Task Queue Task. This is the abstract base class for all
 * Pipeline task types.
 * <p>
 * This class represents both a task to be enqueued and a task being handled.
 * <p>
 * When enqueueing a task, construct a concrete subclass with the appropriate
 * data, and then add the task to an {@link UpdateSpec} and
 * {@link PipelineBackEnd#save(UpdateSpec) save}. Alternatively the task may be
 * enqueued directly using {@link PipelineBackEnd#enqueue(Task)}.
 * <p>
 * When handling a task, construct a {@link Properties} object containing the
 * relevant parameters from the request and then invoke
 * {@link #fromProperties(Properties)}.
 * 
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public abstract class Task {

  protected static final String TASK_TYPE_PARAMETER = "taskType";

  /**
   * The type of task. The Pipeline framework uses several types
   */
  public static enum Type {
    HANDLE_SLOT_FILLED, RUN_JOB, FINALIZE_JOB, FAN_OUT, DELETE_PIPELINE
  }

  protected String taskName;
  protected Type type;
  protected Long delaySeconds;
  protected String onBackend;

  /**
   * This constructor is used on the sending side. That is, it is used to
   * construct a task to be enqueued.
   */
  protected Task(Type t, String name) {
    this.type = t;
    this.taskName = name;
  }

  /**
   * Construct a task from {@code Properties}. This method is used on the
   * receiving side. That is, it is used to construct a {@code Task} from an
   * HttpRequest sent from the App Engine task queue. {@code properties} must
   * contain a property named {@link #TASK_TYPE_PARAMETER}. In addition it must
   * contain the properties specified by the concrete subclass of this class
   * corresponding to the task type.
   */
  public static Task fromProperties(Properties properties) {
    String taskTypeString = properties.getProperty(TASK_TYPE_PARAMETER);
    if (null == taskTypeString) {
      throw new IllegalArgumentException(TASK_TYPE_PARAMETER + " property is missing: "
          + properties.toString());
    }
    Type type = Type.valueOf(taskTypeString);
    switch (type) {
      case HANDLE_SLOT_FILLED:
        return new HandleSlotFilledTask(properties);
      case RUN_JOB:
        return new RunJobTask(properties);
      case FINALIZE_JOB:
        return new FinalizeJobTask(properties);
      case FAN_OUT:
        return new FanoutTask(properties);
      case DELETE_PIPELINE:
        return new DeletePipelineTask(properties);
      default:
        throw new RuntimeException("Unrecognized task type: " + type);
    }
  }

  public Type getType() {
    return type;
  }

  public String getName() {
    return taskName;
  }

  public void setName(String name) {
    taskName = name;
  }

  public void setDelaySeconds(long seconds) {
    this.delaySeconds = seconds;
  }

  public Long getDelaySeconds() {
    return delaySeconds;
  }

  public void setOnBackend(String backend) {
    this.onBackend = backend;
  }

  public String getOnBackend() {
    return onBackend;
  }

  public Properties toProperties() {
    Properties properties = new Properties();
    properties.setProperty(TASK_TYPE_PARAMETER, type.toString());
    addProperties(properties);
    return properties;
  }

  protected abstract void addProperties(Properties properties);



}
