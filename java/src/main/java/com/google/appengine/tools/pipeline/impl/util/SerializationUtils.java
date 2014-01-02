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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * @author rudominer@google.com (Your Name Here)
 *
 */
public class SerializationUtils {

  private static final int MAX_UNCOMPRESSED_BYTE_SIZE = 1000000;
  private static final int ZLIB_COMPRESSION = 1;

  private static class InternalByteArrayOutputStream extends ByteArrayOutputStream {

    public InternalByteArrayOutputStream(int size) {
      super(size);
    }

    private byte[] getInternalBuffer() {
      return buf;
    }
  }

  public static byte[] serialize(Object x) throws IOException {
    InternalByteArrayOutputStream bytes = new InternalByteArrayOutputStream(512);
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(x);
    }
    if (bytes.size() <= MAX_UNCOMPRESSED_BYTE_SIZE) {
      return bytes.toByteArray();
    }
    ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream(bytes.size() / 4);
    compressedBytes.write(0);
    compressedBytes.write(ZLIB_COMPRESSION);
    Deflater deflater =  new Deflater(Deflater.BEST_COMPRESSION, true);
    try (DeflaterOutputStream out = new DeflaterOutputStream(compressedBytes, deflater)) {
      // Use internal buffer to avoid copying it.
      out.write(bytes.getInternalBuffer(), 0, bytes.size());
    } finally {
      deflater.end();
    }
    return compressedBytes.toByteArray();
  }

  public static Object deserialize(byte[] bytes) throws IOException {
    if (bytes.length < 2) {
      throw new IOException("Invalid bytes content");
    }
    InputStream in = new ByteArrayInputStream(bytes);
    if (bytes[0] == 0) {
      in.read(); // consume the marker;
      if (in.read() != ZLIB_COMPRESSION) {
        throw new IOException("Unknown compression type");
      }
      final Inflater inflater =  new Inflater(true);
      in = new InflaterInputStream(in, inflater) {
        @Override public void close() throws IOException {
          try {
            super.close();
          } finally {
            inflater.end();
          }
        }
      };
    }
    try (ObjectInputStream oin = new ObjectInputStream(in)) {
      try {
        return oin.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException("Exception while deserilaizing.", e);
      }
    }
  }
}
