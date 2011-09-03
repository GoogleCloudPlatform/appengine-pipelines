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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public abstract class CascadeModelObject {

  public static final String ROOT_JOB_KEY_PROPERTY = "rootJobKey";

  protected Key key;
  protected Key rootJobKey;

  protected CascadeModelObject(Key rootJobKey, Key key) {
    this.key = key;
    this.rootJobKey = rootJobKey;
    if (null == key) {
      this.key = PipelineManager.getBackEnd().generateKey(this);
    }
    if (null == rootJobKey) {
      if (this instanceof JobRecord && null == key) {
        // This is the root job of a new cascade
        this.rootJobKey = this.key;
      } else {
        throw new IllegalArgumentException("rootJobKey is null");
      }
    }
  }

  protected CascadeModelObject(Key rootJobKey) {
    this(rootJobKey, null);
  }

  protected CascadeModelObject(Entity entity) {
    this(extractRootJobKey(entity), extractKey(entity));
    String expectedEntityType = getDatastoreKind();
    if (!getDatastoreKind().equals(extractType(entity))) {
      throw new IllegalArgumentException("The entity is not of kind " + getDatastoreKind());
    }
  }

  private static Key extractRootJobKey(Entity entity) {
    return (Key) entity.getProperty(ROOT_JOB_KEY_PROPERTY);
  }

  private static String extractType(Entity entity) {
    return entity.getKind();
  }

  private static Key extractKey(Entity entity) {
    return entity.getKey();
  }

  public abstract Entity toEntity();

  protected Entity toProtoEntity() {
    Entity entity = new Entity(key);
    entity.setProperty(ROOT_JOB_KEY_PROPERTY, rootJobKey);
    return entity;
  }

  public Key getKey() {
    return key;
  }

  public Key getRootJobKey() {
    return rootJobKey;
  }

  public abstract String getDatastoreKind();

  protected static <E> List<E> buildInflated(Collection<Key> listOfIds, Map<Key, E> pool) {
    ArrayList<E> list = new ArrayList<E>(listOfIds.size());
    for (Key id : listOfIds) {
      E x = pool.get(id);
      if (null == x) {
        throw new RuntimeException("No object found in pool with id=" + id);
      }
      list.add(x);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  protected static <E> List<E> getListProperty(String propertyName, Entity entity) {
    List<E> list = (List<E>) entity.getProperty(propertyName);
    if (null == list) {
      list = new LinkedList<E>();
    }
    return list;
  }

}
