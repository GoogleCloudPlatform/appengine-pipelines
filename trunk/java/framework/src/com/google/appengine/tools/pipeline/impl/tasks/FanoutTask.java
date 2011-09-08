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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class FanoutTask extends Task {

  private static final String KEY_VALUE_SEPERATOR = "::";
  private static final String PROPERTY_SEPERATOR = ",,";
  private static final String TASK_NAME_DELIMITTER = "--";
  private static final String TASK_SEPERATOR = ";;";
  private static final String RECORD_KEY_PROPERTY = "recordKey";

  private Key recordKey;

  public FanoutTask(Key recordKey) {
    super(Type.FAN_OUT, null);
    this.recordKey = recordKey;
  }

  public FanoutTask(Properties properties) {
    super(Type.FAN_OUT, null);
    String keyString = properties.getProperty(RECORD_KEY_PROPERTY);
    recordKey = KeyFactory.stringToKey(keyString);
  }

  @Override
  protected void addProperties(Properties properties) {
    properties.setProperty(RECORD_KEY_PROPERTY, KeyFactory.keyToString(recordKey));
  }

  public Key getRecordKey() {
    return recordKey;
  }

  public static byte[] encodeTasks(Collection<Task> taskList) {
    StringBuilder builder = new StringBuilder(1024);
    boolean firstTask = true;
    for (Task task : taskList) {
      if (!firstTask) {
        builder.append(TASK_SEPERATOR);
      }
      firstTask = false;
      encodeTask(builder, task);
    }
    return builder.toString().getBytes();
  }

  private static void encodeTask(StringBuilder builder, Task task) {
    String taskName = GUIDGenerator.nextGUID();
    builder.append(taskName);
    builder.append(TASK_NAME_DELIMITTER);
    Properties taskProps = task.toProperties();
    Enumeration<?> propNames = taskProps.propertyNames();
    boolean firstProperty = true;
    while (propNames.hasMoreElements()) {
      if (!firstProperty) {
        builder.append(PROPERTY_SEPERATOR);
      }
      firstProperty = false;
      String propName = (String) propNames.nextElement();
      String value = taskProps.getProperty(propName);
      builder.append(propName).append(KEY_VALUE_SEPERATOR).append(value);
    }
  }

  public static List<Task> decodeTasks(byte[] encodedBytes) {
    String encodedListOfTasks = new String(encodedBytes);
    String[] encodedTaskArray = encodedListOfTasks.split(TASK_SEPERATOR);
    int numTasks = encodedTaskArray.length;
    List<Task> listOfTasks = new ArrayList<Task>(numTasks);
    for (String encodedTask : encodedTaskArray) {
      String[] nameAndProperties = encodedTask.split(TASK_NAME_DELIMITTER);
      String taskName = nameAndProperties[0];
      String encodedProperties = nameAndProperties[1];
      String[] encodedPropertyArray = encodedProperties.split(PROPERTY_SEPERATOR);
      int numProperties = encodedPropertyArray.length;
      Properties taskProperties = new Properties();
      for (String encodedProperty : encodedPropertyArray) {
        String[] keyValuePair = encodedProperty.split(KEY_VALUE_SEPERATOR);
        String key = keyValuePair[0];
        String value = keyValuePair[1];
        taskProperties.setProperty(key, value);
      }
      Task task = Task.fromProperties(taskProperties);
      task.setName(taskName);
      listOfTasks.add(task);
    }
    return listOfTasks;
  }

}
