package com.google.appengine.tools.pipeline.impl.tasks;

import java.util.Properties;

public abstract class Task {
  
  protected static final String TASK_TYPE_PARAMETER = "taskType";
  
  public static enum Type {
    HANDLE_SLOT_FILLED, RUN_JOB, FINALIZE_JOB, FAN_OUT
  }
  
  protected String taskName;
  protected Type type;
  protected Long delaySeconds;
  
  protected Task(Type t, String name){
	  this.type = t;
	  this.taskName = name;
  }
  
  public Type getType(){
	  return type;
  }
  
  public String getName(){
	  return taskName;
  }
  
  public void setName(String name) {
    taskName = name;
  }
  
  public void setDelaySeconds(long seconds) {
    this.delaySeconds = seconds;
  }
  
  public Long getDelaySeconds(){
    return delaySeconds;
  }

  public Properties toProperties(){
    Properties properties = new Properties();
    properties.setProperty(TASK_TYPE_PARAMETER,type.toString());
    addProperties(properties);
    return properties;
  }
  
  protected abstract void addProperties(Properties properties);
  
  public static Task fromProperties(Properties properties){
    String taskTypeString = properties.getProperty(TASK_TYPE_PARAMETER);
    if(null == taskTypeString){
      throw new IllegalArgumentException(TASK_TYPE_PARAMETER+" property is missing: " + properties.toString());
    }
    Type type = Type.valueOf(taskTypeString);
    switch(type){
      case HANDLE_SLOT_FILLED:
        return new HandleSlotFilledTask(properties);
      case RUN_JOB:
        return new RunJobTask(properties);
      case FINALIZE_JOB:
        return new FinalizeJobTask(properties);
      case FAN_OUT:
        return new FanoutTask(properties);
      default:
        throw new RuntimeException("Unrecognized task type: "+type);
    }
  }

}
