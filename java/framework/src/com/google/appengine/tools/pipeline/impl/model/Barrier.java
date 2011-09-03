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
import com.google.appengine.tools.pipeline.impl.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class Barrier extends CascadeModelObject {
  
  /**
   * The type of Barrier
   */
  public static enum Type {
    RUN, FINALIZE
  }

  public static final String DATA_STORE_KIND = "barrier";
  private static final String TYPE_PROPERTY = "barrierType";
  private static final String JOB_KEY_PROPERTY = "jobKey";
  private static final String RELEASED_PROPERTY = "released";
  public static final String WAITING_ON_KEYS_PROPERTY = "waitingOnKeys";
  public static final String WAITING_ON_GROUP_SIZES_PROPERTY = "waitingOnGroupSizes";

  // persistent
  private Type type;
  private Key jobKey;
  private boolean released;
  private List<Key> waitingOnKeys;
  private List<Long> waitingOnGroupSizes;

  // transient
  private List<SlotDescriptor> waitingOnInflated;


  private Barrier(Type type, Key rootJobKey, Key key) {
    super(rootJobKey);
    this.jobKey = key;
    this.type = type;
    this.waitingOnGroupSizes = new LinkedList<Long>();
    this.waitingOnInflated = new LinkedList<SlotDescriptor>();
    this.waitingOnKeys = new LinkedList<Key>();
  }

  public static Barrier dummyInstanceForTesting() {
    Key dummyKey = KeyFactory.createKey("dummy", "dummy");
    return new Barrier(Type.RUN, dummyKey, dummyKey);
  }

  public Barrier(Type type, JobRecord jobRecord) {
    this(type, jobRecord.rootJobKey, jobRecord.key);
  }

  @SuppressWarnings("unchecked")
  public Barrier(Entity entity) {
    super(entity);
    this.jobKey = (Key) entity.getProperty(JOB_KEY_PROPERTY);
    this.type = Type.valueOf((String) entity.getProperty(TYPE_PROPERTY));
    this.released = (Boolean) entity.getProperty(RELEASED_PROPERTY);
    this.waitingOnKeys = getListProperty(WAITING_ON_KEYS_PROPERTY, entity);
    this.waitingOnGroupSizes = getListProperty(WAITING_ON_GROUP_SIZES_PROPERTY, entity);
  }

  @Override
  public Entity toEntity() {
    Entity entity = toProtoEntity();
    entity.setProperty(JOB_KEY_PROPERTY, jobKey);
    entity.setProperty(TYPE_PROPERTY, type.toString());
    entity.setProperty(RELEASED_PROPERTY, released);
    entity.setProperty(WAITING_ON_KEYS_PROPERTY, waitingOnKeys);
    entity.setProperty(WAITING_ON_GROUP_SIZES_PROPERTY, waitingOnGroupSizes);
    return entity;
  }

  @Override
  public String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public void inflate(Map<Key, Slot> pool) {
    int numSlots = waitingOnKeys.size();
    waitingOnInflated = new ArrayList<SlotDescriptor>(numSlots);
    for (int i = 0; i < numSlots; i++) {
      Key key = waitingOnKeys.get(i);
      int groupSize = waitingOnGroupSizes.get(i).intValue();
      Slot slot = pool.get(key);
      if (null == slot) {
        throw new RuntimeException("No slot in pool with key=" + key);
      }
      SlotDescriptor descriptor = new SlotDescriptor(slot, groupSize);
      waitingOnInflated.add(descriptor);
    }
  }

  public Key getJobKey() {
    return jobKey;
  }

  public Type getType() {
    return type;
  }

  public boolean isReleased() {
    return released;
  }

  public void setReleased() {
    released = true;
  }

  public List<Key> getWaitingOnKeys() {
    return waitingOnKeys;
  }

  /**
   * May return null if this Barrier has not been inflated
   */
  public List<SlotDescriptor> getWaitingOnInflated() {
    return waitingOnInflated;
  }

  public Object[] buildArgumentArray() {
    List<Object> argumentList = buildArgumentList();
    Object[] argumentArray = new Object[argumentList.size()];
    argumentList.toArray(argumentArray);
    return argumentArray;
  }

  // Visible for testing
  public List<Object> buildArgumentList() {
    if (null == waitingOnInflated) {
      throw new RuntimeException("" + this + " has not been inflated.");
    }
    List<Object> argumentList = new LinkedList<Object>();
    ArrayList<Object> currentListArg = null;
    int currentListArgExpectedSize = -1;
    for (SlotDescriptor descriptor : waitingOnInflated) {
      if (!descriptor.slot.isFilled()) {
        throw new RuntimeException("Slot is not filled: " + descriptor.slot);
      }
      Object nextValue = descriptor.slot.getValue();
      if (currentListArg != null) {
        // Assert: currentListArg.size() < currentListArgExpectedSize
        if (descriptor.groupSize != currentListArgExpectedSize) {
          throw new RuntimeException("expectedGroupSize: " + currentListArgExpectedSize
              + ", groupSize: " + descriptor.groupSize + "; nextValue=" + nextValue);
        }
        currentListArg.add(nextValue);
      } else {
        if (descriptor.groupSize > 0) {
          // We are not in the midst of a list and this element starts a new
          // list
          currentListArgExpectedSize = descriptor.groupSize;
          currentListArg = new ArrayList<Object>(currentListArgExpectedSize);
          currentListArg.add(nextValue);
          argumentList.add(currentListArg);
        } else if (descriptor.groupSize == 0) {
          // We are not in the midst of a list and this element is not part of
          // a list
          argumentList.add(nextValue);
        } else {
          // We were not in the midst of a list and this element is phantom
        }
      }
      if (null != currentListArg && currentListArg.size() == currentListArgExpectedSize) {
        // We have finished with the currentListArg
        currentListArg = null;
        currentListArgExpectedSize = -1;
      }
    }
    return argumentList;
  }

  private void addSlotDescriptor(SlotDescriptor slotDescr) {
    if (null == waitingOnInflated) {
      waitingOnInflated = new LinkedList<SlotDescriptor>();
    }
    waitingOnInflated.add(slotDescr);
    waitingOnGroupSizes.add((long) slotDescr.groupSize);
    Slot slot = slotDescr.slot;
    slot.addWaiter(this);
    waitingOnKeys.add(slotDescr.slot.key);
  }

  public void addRegularArgumentSlot(Slot slot) {
    addSlotDescriptor(new SlotDescriptor(slot, 0));
  }

  public void addPhantomArgumentSlot(Slot slot) {
    addSlotDescriptor(new SlotDescriptor(slot, -1));
  }

  public void addListArgumentSlots(List<Slot> slotList) {
    int groupSize = slotList.size();
    if (groupSize < 1) {
      throw new IllegalArgumentException("slotList must have positive length.");
    }
    for (Slot slot : slotList) {
      addSlotDescriptor(new SlotDescriptor(slot, groupSize));
    }
  }

  @Override
  public String toString() {
    return "Barrier[" + key.getName() + ", " + type + ", released=" + released + ", "
        + jobKey.getName() + ", waitingOn="
        + StringUtils.toStringParallel(waitingOnKeys, waitingOnGroupSizes) + "]";
  }

}
