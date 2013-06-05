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

  public HandleChildExceptionTask(Key jobKey, Key failedChildKey) {
    super(Type.HANDLE_CHILD_EXCEPTION, "handleChildFailure", jobKey);
    if (null == failedChildKey) {
      throw new NullPointerException("failedChildKey");
    }
    this.failedChildKey = failedChildKey;
  }

  public HandleChildExceptionTask(Properties properties) {
    super(Type.HANDLE_CHILD_EXCEPTION, properties);
    String failedChild = properties.getProperty(FAILED_CHILD_KEY_PARAM);
    this.failedChildKey = KeyFactory.stringToKey(failedChild);
  }

  @Override
  protected void addProperties(Properties properties) {
    super.addProperties(properties);
    properties.setProperty(FAILED_CHILD_KEY_PARAM, KeyFactory.keyToString(failedChildKey));
  }

  /**
   * @return the failedChildKey
   */
  public Key getFailedChildKey() {
    return failedChildKey;
  }
  
}
