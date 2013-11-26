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
import com.google.appengine.api.datastore.KeyFactory;

/**
 * A data model to represent shard of a large value.
 *
 * @author ozarov@google.com (Arie Ozarov)
 *
 */
public class ShardedValue extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-sharded-value";
  private static final String SHARD_ID_PROPERTY = "shard-id";
  private static final String VALUE_PROPERTY = "value";

  private final long shardId;
  private final byte[] value;

  public ShardedValue(PipelineModelObject parent, String property, long shardId, byte[] value) {
    super(parent.getRootJobKey(), null, createKey(parent, property, shardId),
        parent.getGeneratorJobKey(), parent.getGraphGuid());
    this.shardId = shardId;
    this.value = value;
  }

  private static Key createKey(PipelineModelObject parent, String property, long shardId) {
    return KeyFactory.createKey(parent.getKey(), DATA_STORE_KIND, property + "." + shardId);
  }

  public ShardedValue(Entity entity) {
    super(entity);
    this.shardId = (Long) entity.getProperty(SHARD_ID_PROPERTY);
    this.value = ((Blob) entity.getProperty(VALUE_PROPERTY)).getBytes();
  }

  @Override
  public Entity toEntity() {
    Entity entity = toProtoEntity();
    entity.setProperty(SHARD_ID_PROPERTY, shardId);
    entity.setUnindexedProperty(VALUE_PROPERTY, new Blob(value));
    return entity;
  }

  @Override
  public String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public long getShardId() {
    return shardId;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "ShardedValue[" + key.getName() + ", shardId=" + shardId + ", value="
        + getValueForDisplay() + "]";
  }

  private String getValueForDisplay() {
    int count = 0;
    StringBuilder stBuilder = new StringBuilder(103);
    for (byte b : value) {
      if (++count == 100) {
        stBuilder.append("...");
        break;
      }
      stBuilder.append(String.format("%02x", b & 0xFF));
    }
    return stBuilder.toString();
  }
}
