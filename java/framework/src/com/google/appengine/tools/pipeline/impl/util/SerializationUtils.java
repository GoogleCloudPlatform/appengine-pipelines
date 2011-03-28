// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author rudominer@google.com (Your Name Here)
 *
 */
public class SerializationUtils {

  public static byte[] serialize(Object x) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(512);
    ObjectOutputStream out = new ObjectOutputStream(bytes);
    out.writeObject(x);
    return bytes.toByteArray();
  }

  public static Object deserialize(byte[] bytes) throws IOException {
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      return in.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Exception while deserilaizing.", e);
    }
  }
}
