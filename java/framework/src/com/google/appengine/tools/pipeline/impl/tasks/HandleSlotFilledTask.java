package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.impl.model.Slot;

import java.util.Properties;

public class HandleSlotFilledTask extends ObjRefTask {

  public HandleSlotFilledTask(Key slotKey, String name) {
    super(Type.HANDLE_SLOT_FILLED, name, slotKey);
  }

  public HandleSlotFilledTask(Slot slot) {
    this(slot.getKey(), null);
  }

  public HandleSlotFilledTask(Properties properties) {
    super(Type.HANDLE_SLOT_FILLED, properties);
  }

  public Key getSlotKey() {
    return key;
  }

}
