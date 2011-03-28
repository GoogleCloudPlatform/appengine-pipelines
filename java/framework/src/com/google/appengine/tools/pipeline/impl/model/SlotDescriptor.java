package com.google.appengine.tools.pipeline.impl.model;

/**
 * A structure used to describe the role that a {@link Slot} plays in a
 * {@link Barrier}.
 * <p>
 * There are three roles a {@code Slot} might play:
 * <ol>
 * <li> A single argument.
 * <li> A member of a {@code List} argument. These are used to implement
 *  the {@code futureList()} feature in which {@code List} of {@code FutureValues}
 *  may be interpretted as a {@code FutureValue<List>}.
 * <li> A phantom argument. These are used to implement the {@code waitFor} feature.
 * The {@code Barrier} is waiting for the {@code Slot} to be filled, but the value
 * of the {@code Slot} will be ignored, it will not be passed to the
 * {@code run()} method.
 * </ol>
 * <p>
 * We use the {@link #groupSize} field to encode the role as follows:
 * <ul>
 * <li> groupSize < 0 : The slot is phantom
 * <li> groupSize = 0 : The slot is a single argument
 * <li> groupSize > 0 : The slot is a member of a list of length groupSize
 * </ul>
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
