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
