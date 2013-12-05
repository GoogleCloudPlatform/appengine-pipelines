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
import com.google.appengine.tools.pipeline.impl.QueueSettings;

import java.util.Properties;

/**
 * A subclass of {@code ObjRefTask} used for the purpose of requesting
 * that the pipeline with the specified root job key should be deleted.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class DeletePipelineTask extends ObjRefTask {

  /**
   * A parameter specifying that the Pipeline should be deleted no matter
   *  what state it is in.
   */
  public static final String FORCE_PARAM = "force";

  private final boolean force;

  public DeletePipelineTask(Key rootJobKey, boolean force, QueueSettings queueSettings) {
    super(Type.DELETE_PIPELINE, "deletePipeline", rootJobKey, queueSettings);
    this.force = force;
  }

  protected DeletePipelineTask(Type type, String taskName, Properties properties) {
    super(type, taskName, properties);
    force = Boolean.parseBoolean(properties.getProperty(FORCE_PARAM));
  }

  @Override
  protected void addProperties(Properties properties) {
    super.addProperties(properties);
    properties.setProperty(FORCE_PARAM, Boolean.toString(force));
  }

  @Override
  public String propertiesAsString() {
    return super.propertiesAsString() + ", force=" + force;
  }

  public Key getRootJobKey() {
    return getKey();
  }

  public boolean shouldForce() {
    return force;
  }
}
