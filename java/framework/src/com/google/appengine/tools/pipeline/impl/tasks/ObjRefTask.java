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
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public abstract class ObjRefTask extends Task {

  private static final String KEY_PARAM = "key";

  protected Key key;

  protected ObjRefTask(Type type, String name, Key key) {
    super(type, name);
    if (null == key) {
      throw new IllegalArgumentException("key is null.");
    }
    this.key = key;
  }

  public ObjRefTask(Type type, Properties properties) {
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
