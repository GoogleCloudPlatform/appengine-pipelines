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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.impl.model.Slot;

/**
 * Implements {@link PromisedValue} by wrapping a newly created {@link Slot}
 * which will contain the promised value when it is delivered.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 *
 * @param <E> The type of the value represented by this {@code PromisedValue}
 */
public class PromisedValueImpl<E> extends FutureValueImpl<E> implements PromisedValue<E> {

  public PromisedValueImpl(Key rootJobGuid, Key generatorJobKey, String graphGUID) {
    super(new Slot(rootJobGuid, generatorJobKey, graphGUID));
  }

  @Override
  public String getHandle() {
    return KeyFactory.keyToString(slot.getKey());
  }

}
