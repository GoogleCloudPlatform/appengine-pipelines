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
 * A subclass of {@link ObjRefTask} used to implement delayed value.
 * @see com.google.appengine.tools.pipeline.Job#newDelayedValue(long)
 * 
 * @author maximf@google.com (Maxim Fateev)
 */
public class FillSlotHandleSlotFilledTask extends ObjRefTask {
  
  private static final String ROOT_JOB_KEY_PARAM = "rootJobKey";
  
  private Key rootJobKey;

  /**
   * This constructor is used on the sending side. That is, it is used to
   * construct a {@code HandleSlotFilledTask}to be enqueued.
   * <p>
   * 
   * @param slotKey The key of the Slot whose filling is to be handled
   * @param rootJobKey The key of the root job of the pipeline
   */
  public FillSlotHandleSlotFilledTask(Key slotKey, Key rootJobKey) {
    super(Type.FILL_SLOT_HANDLE_SLOT_FILLED, "fillSlotHandleSlotFilled", slotKey);
    this.rootJobKey = rootJobKey;
  }

  /**
   * This constructor is used on the receiving side. That is, it is used to
   * construct a {@code FillSlotHandleSlotFilledTask} from an HttpRequest sent from the
   * App Engine task queue.
   * 
   * @param properties See the requirements on {@code properties} specified
   * in the parent class constructor.
   */
  public FillSlotHandleSlotFilledTask(Properties properties) {
    super(Type.FILL_SLOT_HANDLE_SLOT_FILLED, properties);
    rootJobKey = KeyFactory.stringToKey(properties.getProperty(ROOT_JOB_KEY_PARAM));
  }

  @Override
  protected void addProperties(Properties properties) {
    super.addProperties(properties);
    properties.setProperty(ROOT_JOB_KEY_PARAM, KeyFactory.keyToString(rootJobKey));
  }

  public Key getSlotKey() {
    return key;
  }

  public Key getRootJobKey() {
    return rootJobKey;
  }

}
