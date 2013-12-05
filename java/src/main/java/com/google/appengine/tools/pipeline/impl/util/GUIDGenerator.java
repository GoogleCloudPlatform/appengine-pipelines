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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class GUIDGenerator {
  private static AtomicInteger counter = new AtomicInteger();

  public static final String USE_SIMPLE_GUIDS_FOR_DEBUGGING =
      "com.google.appengine.api.pipeline.use-simple-guids-for-debugging";

  public static synchronized String nextGUID() {
    if (Boolean.getBoolean(USE_SIMPLE_GUIDS_FOR_DEBUGGING)) {
      return "" + counter.getAndIncrement();
    }
    UUID uuid = UUID.randomUUID();
    return uuid.toString();
  }

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++) {
      System.out.println(nextGUID());
    }
  }
}
