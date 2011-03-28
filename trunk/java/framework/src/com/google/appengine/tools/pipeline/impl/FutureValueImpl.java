package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.impl.model.Slot;

public class FutureValueImpl<E> implements FutureValue<E> {
  protected Slot slot;

  public FutureValueImpl(Slot slt) {
    this.slot = slt;
  }

  public Slot getSlot() {
    return slot;
  }
  
  public String getSourceJobHandle() {
    return KeyFactory.keyToString(slot.getSourceJobKey());
  }
  
  public String getPipelineHandle() {
    return KeyFactory.keyToString(slot.getRootJobKey());
  }

  @Override
  public String toString() {
    return "FutureValue[" + slot + "]";
  }
}
