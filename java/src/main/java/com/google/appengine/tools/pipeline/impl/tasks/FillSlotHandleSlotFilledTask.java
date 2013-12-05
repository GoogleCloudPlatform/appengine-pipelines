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
 * A subclass of {@link ObjRefTask} used to implement delayed value.
 * @see com.google.appengine.tools.pipeline.Job#newDelayedValue(long)
 *
 * @author maximf@google.com (Maxim Fateev)
 */
public class FillSlotHandleSlotFilledTask extends ObjRefTask {

  private static final String ROOT_JOB_KEY_PARAM = "rootJobKey";

  private final Key rootJobKey;

  /**
   * This constructor is used on the sending side. That is, it is used to
   * construct a {@code HandleSlotFilledTask}to be enqueued.
   * <p>
   *
   * @param slotKey The key of the Slot whose filling is to be handled
   * @param rootJobKey The key of the root job of the pipeline
   */
  public FillSlotHandleSlotFilledTask(Key slotKey, Key rootJobKey, QueueSettings queueSettings) {
    super(Type.FILL_SLOT_HANDLE_SLOT_FILLED, "fillSlotHandleSlotFilled", slotKey, queueSettings);
    this.rootJobKey = rootJobKey;
  }

  protected FillSlotHandleSlotFilledTask(Type type, String taskName, Properties properties) {
    super(type, taskName, properties);
    rootJobKey = KeyFactory.stringToKey(properties.getProperty(ROOT_JOB_KEY_PARAM));
  }

  @Override
  protected void addProperties(Properties properties) {
    super.addProperties(properties);
    properties.setProperty(ROOT_JOB_KEY_PARAM, KeyFactory.keyToString(rootJobKey));
  }

  @Override
  public String propertiesAsString() {
    return super.propertiesAsString() + ", rootJobKey=" + rootJobKey;
  }

  public Key getSlotKey() {
    return getKey();
  }

  public Key getRootJobKey() {
    return rootJobKey;
  }
}
