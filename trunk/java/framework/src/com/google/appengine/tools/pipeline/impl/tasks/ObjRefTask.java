// Copyright 2011 Google Inc. All Rights Reserved.

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
    if (null != delaySeconds){
      delayString = " delaySeconds="+delaySeconds;
    }
    return type.toString() + "_TASK[" + key + "]" + nameString + delayString;
  }
}
