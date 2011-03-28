// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class FanoutTask extends Task {
  
  private static final String DELIMITER = "-";
  // private static final String TASK_NAME_PROPERTY = "taskName";

  List<Task> listOfTasks;

  public FanoutTask(List<Task> taskList) {
    super(Type.FAN_OUT, null);
    this.listOfTasks = taskList;
  }

  public FanoutTask(Properties properties) {
    super(Type.FAN_OUT, null);
    Enumeration<?> propNames = properties.propertyNames();
    Map<String, Properties> taskPropertiesMap = new HashMap<String, Properties>();
    while (propNames.hasMoreElements()) {
      String compoundPropName = (String) propNames.nextElement();
      String value = properties.getProperty(compoundPropName);
      String[] parts = compoundPropName.split(DELIMITER, 2);
      if (parts.length == 2) {
        String propName = parts[0];
        String taskName = parts[1];
        Properties taskProperties = taskPropertiesMap.get(taskName);
        if (null == taskProperties) {
          taskProperties = new Properties();
          taskPropertiesMap.put(taskName, taskProperties);
        }
        taskProperties.put(propName, value);
      }
    }
    listOfTasks = new ArrayList<Task>(taskPropertiesMap.size());
    for (Entry<String, Properties> entry : taskPropertiesMap.entrySet()) {
      String taskName = entry.getKey();
      Properties taskProperties = entry.getValue();
      Task task = Task.fromProperties(taskProperties);
      task.setName(taskName);
      listOfTasks.add(task);
    }
  }

  @Override
  protected void addProperties(Properties properties) {
    for (Task task : listOfTasks) {
      String taskName = GUIDGenerator.nextGUID();
      Properties taskProps = task.toProperties();
      Enumeration<?> propNames = taskProps.propertyNames();
      while (propNames.hasMoreElements()) {
        String propName = (String) propNames.nextElement();
        String value = taskProps.getProperty(propName);
        String compoundPropName = propName + DELIMITER + taskName;
        properties.put(compoundPropName, value);
      }
    }
  }

  public List<Task> getTasks() {
    return listOfTasks;
  }

  @Override
  public String toString() {
    return type.toString() + " Tasks: " + listOfTasks;
  }

}
