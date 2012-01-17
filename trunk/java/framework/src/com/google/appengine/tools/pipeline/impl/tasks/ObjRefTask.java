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

import java.util.Properties;

/**
 * A subclass of {@code Task} for tasks which need to reference a particular
 * Pipeline model object. The task contains the data store key of the reference
 * object in its {@link #KEY_PARAM} property.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public abstract class ObjRefTask extends Task {

  private static final String KEY_PARAM = "key";

  /**
   * The {@code Key} of the object to which this {@code ObjRefTask} refers.
   */
  protected Key key;


  /**
   * This constructor is used on the sending side. That is, it is used to
   * construct a task to be enqueued.
   * 
   * @param type The type of task being constructed
   * @param namePrefix If this is {@code null} it means the task will not have a
   *        name. Otherwise this will be a prefix of the task name.
   * @param key The {@code Key} of the object to which this {@code ObjRefTask}
   *        will refer. It will be used as part of the task name if
   *        {@code namePrefix} is not {@code null}.
   */
  protected ObjRefTask(Type type, String namePrefix, Key key) {
    super(type, (null == namePrefix ? null : namePrefix + KeyFactory.keyToString(key)));
    if (null == key) {
      throw new IllegalArgumentException("key is null.");
    }
    this.key = key;
  }

  /**
   * This constructor is used on the receiving side. That is, it is used to
   * construct an {@code ObjRefTask} from an HttpRequest sent from the App
   * Engine task queue.
   * 
   * @param type The type of task being constructed
   * @param properties In addition to the properties specified in the parent
   *        constructor, {@code properties} must also contain a property named "key"
   *        with a value of a stringified data store key. This will be used as
   *        the {@link #key} of the object to which this {@code ObjRefTask}
   *        refers.
   */
  protected ObjRefTask(Type type, Properties properties) {
    this(type, null, KeyFactory.stringToKey(properties.getProperty(KEY_PARAM)));
  }

  public Key getKey() {
    return key;
  }

  @Override
  protected void addProperties(Properties properties) {
    String keyString = KeyFactory.keyToString(key);
    properties.setProperty(KEY_PARAM, keyString);
  }

  @Override
  public String toString() {
    String nameString = "";
    if (null != taskName) {
      nameString = "taskName(" + taskName + ")";
    }
    String delayString = "";
    if (null != delaySeconds) {
      delayString = " delaySeconds=" + delaySeconds;
    }
    return type.toString() + "_TASK[" + key + "]" + nameString + delayString;
  }
}
