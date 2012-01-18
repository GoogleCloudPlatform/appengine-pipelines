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

package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;

import java.io.IOException;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class JobInstanceRecord extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "jobInstanceRecord";
  public static final String JOB_KEY_PROPERTY = "jobKey";
  public static final String JOB_CLASS_NAME_PROPERTY = "jobClassName";
  public static final String INSTANCE_BYTES_PROPERTY = "bytes";

  // persistent
  private Key jobKey;
  private String jobClass;
  private byte[] instanceBytes;

  // transient
  private Job<?> jobInstance;

  public JobInstanceRecord(JobRecord job, Job<?> jobInstance) {
    super(job.rootJobKey, job.generatorJobKey, job.graphGUID);
    jobKey = job.key;
    this.jobInstance = jobInstance;
    this.jobClass = jobInstance.getClass().getName();
    try {
      instanceBytes = SerializationUtils.serialize(jobInstance);
    } catch (IOException e) {
      throw new RuntimeException("Exception while attempting to serialize the jobInstance "
          + jobInstance, e);
    }
  }

  public JobInstanceRecord(Entity entity) {
    super(entity);
    jobKey = (Key) entity.getProperty(JOB_KEY_PROPERTY);
    jobClass = (String) entity.getProperty(JOB_CLASS_NAME_PROPERTY);
    instanceBytes = ((Blob) entity.getProperty(INSTANCE_BYTES_PROPERTY)).getBytes();
  }

  @Override
  public Entity toEntity() {
    Entity entity = toProtoEntity();
    entity.setProperty(INSTANCE_BYTES_PROPERTY, new Blob(instanceBytes));
    entity.setProperty(JOB_KEY_PROPERTY, jobKey);
    entity.setProperty(JOB_CLASS_NAME_PROPERTY, jobClass);
    return entity;
  }

  @Override
  public String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public Key getJobKey() {
    return jobKey;
  }

  public String getJobClass() {
    return jobClass;
  }

  public synchronized Job<?> getJobInstanceDeserialized() {
    if (null == jobInstance) {
      try {
        jobInstance = (Job<?>) SerializationUtils.deserialize(instanceBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return jobInstance;
  }

}
