// Copyright 2013 Google Inc.
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
import com.google.appengine.tools.pipeline.impl.QueueSettings;

import java.util.Properties;

/**
 * A subclass of {@link ObjRefTask} used to report that the job with the
 * specified key should handle its child failure.
 *
 * @author maximf@google.com (Maxim Fateev)
 *
 */
public class HandleChildExceptionTask extends ObjRefTask {

  private static final String FAILED_CHILD_KEY_PARAM = "failedChildKey";

  private final Key failedChildKey;

  public HandleChildExceptionTask(Key jobKey, Key failedChildKey, QueueSettings queueSettings) {
    super(Type.HANDLE_CHILD_EXCEPTION, "handleChildFailure", jobKey, queueSettings);
    if (null == failedChildKey) {
      throw new NullPointerException("failedChildKey");
    }
    this.failedChildKey = failedChildKey;
  }

  protected HandleChildExceptionTask(Type type, String taskName, Properties properties) {
    super(type, taskName, properties);
    failedChildKey = KeyFactory.stringToKey(properties.getProperty(FAILED_CHILD_KEY_PARAM));
  }

  @Override
  protected void addProperties(Properties properties) {
    super.addProperties(properties);
    properties.setProperty(FAILED_CHILD_KEY_PARAM, KeyFactory.keyToString(failedChildKey));
  }

  @Override
  public String propertiesAsString() {
    return super.propertiesAsString() + ", failedChildKey=" + failedChildKey;
  }

  /**
   * @return the failedChildKey
   */
  public Key getFailedChildKey() {
    return failedChildKey;
  }
}
