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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.impl.PipelineManager;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A slot to be filled in with a value.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class Slot extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "pipeline-slot";
  private static final String FILLED_PROPERTY = "filled";
  private static final String VALUE_PROPERTY = "value";
  private static final String WAITING_ON_ME_PROPERTY = "waitingOnMe";
  private static final String FILL_TIME_PROPERTY = "fillTime";
  private static final String SOURCE_JOB_KEY_PROPERTY = "sourceJob";

  // persistent
  private boolean filled;
  private Date fillTime;
  private Object value;
  private Key sourceJobKey;
  private final List<Key> waitingOnMeKeys;

  // transient
  private List<Barrier> waitingOnMeInflated;

  public Slot(Key rootJobKey, Key generatorJobKey, String graphGUID) {
    super(rootJobKey, generatorJobKey, graphGUID);
    waitingOnMeKeys = new LinkedList<>();
  }

  public Slot(Entity entity) {
    super(entity);
    filled = (Boolean) entity.getProperty(FILLED_PROPERTY);
    fillTime = (Date) entity.getProperty(FILL_TIME_PROPERTY);
    sourceJobKey = (Key) entity.getProperty(SOURCE_JOB_KEY_PROPERTY);
    Object serializedVersion = entity.getProperty(VALUE_PROPERTY);
    try {
      value = PipelineManager.getBackEnd().deserializeValue(this, serializedVersion);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    waitingOnMeKeys = getListProperty(WAITING_ON_ME_PROPERTY, entity);
  }

  @Override
  public Entity toEntity() {
    Entity entity = toProtoEntity();
    entity.setUnindexedProperty(FILLED_PROPERTY, filled);
    if (null != fillTime) {
      entity.setUnindexedProperty(FILL_TIME_PROPERTY, fillTime);
    }
    if (null != sourceJobKey) {
      entity.setProperty(SOURCE_JOB_KEY_PROPERTY, sourceJobKey);
    }
    try {
      entity.setUnindexedProperty(VALUE_PROPERTY,
          PipelineManager.getBackEnd().serializeValue(this, value));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    entity.setProperty(WAITING_ON_ME_PROPERTY, waitingOnMeKeys);
    return entity;
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public void inflate(Map<Key, Barrier> pool) {
    waitingOnMeInflated = buildInflated(waitingOnMeKeys, pool);
  }

  public void addWaiter(Barrier waiter) {
    waitingOnMeKeys.add(waiter.getKey());
    if (null == waitingOnMeInflated) {
      waitingOnMeInflated = new LinkedList<>();
    }
    waitingOnMeInflated.add(waiter);
  }

  public boolean isFilled() {
    return filled;
  }

  public Object getValue() {
    return value;
  }

  /**
   * Will return {@code null} if this slot is not filled.
   */
  public Date getFillTime() {
    return fillTime;
  }

  public Key getSourceJobKey() {
    return sourceJobKey;
  }

  public void setSourceJobKey(Key key) {
    sourceJobKey = key;
  }

  public void fill(Object value) {
    filled = true;
    this.value = value;
    fillTime = new Date();
  }

  public List<Key> getWaitingOnMeKeys() {
    return waitingOnMeKeys;
  }

  /**
   * If this slot has not yet been inflated this method returns null;
   */
  public List<Barrier> getWaitingOnMeInflated() {
    return waitingOnMeInflated;
  }

  @Override
  public String toString() {
    return "Slot[" + getKey().getName() + ", value=" + value + ", filled=" + filled
        + ", waitingOnMe=" + waitingOnMeKeys + "]";
  }
}
