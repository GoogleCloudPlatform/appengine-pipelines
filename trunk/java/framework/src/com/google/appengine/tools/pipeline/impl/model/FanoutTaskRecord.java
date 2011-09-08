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

/**
 * A datastore entity for storing data necessary for a fan-out task
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class FanoutTaskRecord extends CascadeModelObject {
  
  public static final String DATA_STORE_KIND = "fanoutTask";
  private static final String PAYLOAD_PROPERTY = "payload";
  
  private byte[] payload;
  
  public FanoutTaskRecord(Key rootJobKey, byte[] payload){
    super(rootJobKey, null, null);
    this.payload = payload;
  }
  
  public FanoutTaskRecord(Entity entity){
    super(entity);
    Blob payloadBlob = (Blob) entity.getProperty(PAYLOAD_PROPERTY);
    payload = payloadBlob.getBytes();
  }
  
  public byte[] getPayload() {
    return payload;
  }

  
  @Override
  public String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  @Override
  public Entity toEntity() {
    Entity entity = toProtoEntity();
    entity.setProperty(PAYLOAD_PROPERTY, new Blob(payload));
    return entity;
  }
}
