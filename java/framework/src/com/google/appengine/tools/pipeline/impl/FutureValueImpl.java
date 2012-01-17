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

package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.impl.model.Slot;

/**
 * An implementation of {@link FutureValue} that wraps a {@link Slot}.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 *
 * @param <E> The type of the value represented by this {@code FutureValue}
 */
public class FutureValueImpl<E> implements FutureValue<E> {
  protected Slot slot;

  public FutureValueImpl(Slot slt) {
    this.slot = slt;
  }

  public Slot getSlot() {
    return slot;
  }

  @Override
  public String getSourceJobHandle() {
    return KeyFactory.keyToString(slot.getSourceJobKey());
  }

  @Override
  public String getPipelineHandle() {
    return KeyFactory.keyToString(slot.getRootJobKey());
  }

  @Override
  public String toString() {
    return "FutureValue[" + slot + "]";
  }
}
