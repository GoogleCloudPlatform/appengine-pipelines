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
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;

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

  /**
   * Construct a new PipelineModelObject from the provided data.
   * 
   * @param rootJobKey The key of the root job for this pipeline. This must be
   *        non-null, except in the case that we are currently constructing the
   *        root job. In that case {@code thisKey} and {@code parentKey} must
   *        both be null and this must be a {@link JobRecord}.
   * @param parentKey The entity group parent key. This must be null unless
   *        {@code thisKey} is null. If {@code thisKey} is null then {@code
   *        parentKey} will be used to construct {@code thisKey}. {@code
   *        parentKey} and {@code thisKey} are both allowed to be null, in which
   *        case {@code thisKey} will be constructed without a parent.
   * @param thisKey The key for the object being constructed. If this is null
   *        then a new key will be constructed.
   */
  protected CascadeModelObject(Key rootJobKey, Key parentKey, Key thisKey) {
    this.key = thisKey;
    this.rootJobKey = rootJobKey;
    if (null == rootJobKey) {
      if (this instanceof JobRecord) {
        // We are constructing the root job of a new cascade
        if (null != thisKey) {
          throw new IllegalArgumentException("rootJobKey is null and thisKey is not null");
        }
        if (null != parentKey) {
          throw new IllegalArgumentException("rootJobKey is null and parentKey is not null");
        }
      } else {
        throw new IllegalArgumentException("rootJobKey is null");
      }
    } 
    if (null == key) {
      this.key = generateKey(parentKey, getDatastoreKind());
    } else if (parentKey != null) {
      throw new IllegalArgumentException("You may not specify both thisKey and parentKey");
    }
    if (null == rootJobKey) {
      // This is the root job of a new cascade
      this.rootJobKey = this.key;
    }
  }

  /**
   * Construct a new PipelineModelObject with the given rootJobKey as the parent
   * key.
   * 
   * @param rootJobKey Key of the root job. Must not be null.
   */
  protected CascadeModelObject(Key rootJobKey) {
    this(rootJobKey, rootJobKey, null);
  }

  protected CascadeModelObject(Entity entity) {
    this(extractRootJobKey(entity), null, extractKey(entity));
    String expectedEntityType = getDatastoreKind();
    if (!getDatastoreKind().equals(extractType(entity))) {
      throw new IllegalArgumentException("The entity is not of kind " + getDatastoreKind());
    }
  }

  private static Key generateKey(Key parentKey, String kind) {
    String name = GUIDGenerator.nextGUID();
    Key key;
    if (null == parentKey) {
      key = KeyFactory.createKey(kind, name);
    } else {
      key = parentKey.getChild(kind, name);
    }
    return key;
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
