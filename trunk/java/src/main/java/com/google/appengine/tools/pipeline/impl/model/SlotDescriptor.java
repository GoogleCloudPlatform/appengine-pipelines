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

/**
 * A structure used to describe the role that a {@link Slot} plays in a
 * {@link Barrier}.
 * <p>
 * There are three roles a {@code Slot} might play:
 * <ol>
 * <li>A single argument.
 * <li>A member of a {@code List} argument. These are used to implement the
 * {@code futureList()} feature in which {@code List} of {@code FutureValues}
 * may be interpreted as a {@code FutureValue<List>}.
 * <li>A phantom argument. These are used to implement the {@code waitFor}
 * feature. The {@code Barrier} is waiting for the {@code Slot} to be filled,
 * but the value of the {@code Slot} will be ignored, it will not be passed to
 * the {@code run()} method.
 * </ol>
 * <p>
 * We use the {@link #groupSize} field to encode the role as follows:
 * <ul>
 * <li>groupSize < 0 : The slot is phantom
 * <li>groupSize = 0 : The slot is a single argument
 * <li>groupSize > 0 : The slot is a member of a list of length groupSize - 1. The
 *                     first slot in the group is ignored. This allows us to encode
 *                     lists of length zero.
 * </ul>
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class SlotDescriptor {
  public Slot slot;
  public int groupSize;

  SlotDescriptor(Slot sl, int groupSize) {
    this.slot = sl;
    this.groupSize = groupSize;
  }

  public boolean isPhantom() {
    return groupSize < 0;
  }

  public boolean isSingleArgument() {
    return groupSize == 0;
  }

  public boolean isListElement() {
    return groupSize > 0;
  }

  @Override
  public String toString() {
    return "SlotDescriptor [" + slot.key + "," + groupSize + "]";
  }

}
