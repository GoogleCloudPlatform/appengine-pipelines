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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class StringUtils {
  public static String printStackTraceToString(Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    t.printStackTrace(pw);
    pw.flush();
    sw.flush();
    return sw.toString();
  }

  public static String toString(Object x) {
    if (x instanceof Key) {
      return ((Key) x).getName();
    }
    return x.toString();
  }

  public static String toString(Object[] array) {
    StringBuilder builder = new StringBuilder(1024);
    builder.append('[');
    boolean first = true;
    for (Object x : array) {
      if (!first) {
        builder.append(", ");
      }
      first = false;
      builder.append(toString(x));
    }
    builder.append(']');
    return builder.toString();
  }

  public static <E, F> String toStringParallel(List<E> listA, List<F> listB) {
    int length = listA.size();
    if (length != listB.size()) {
      throw new IllegalArgumentException("The two lists must have the same length.");
    }
    StringBuilder builder = new StringBuilder(1024);
    builder.append('<');
    int i = 0;
    for (E x : listA) {
      F y = listB.get(i++);
      if (i > 1) {
        builder.append(", ");
      }
      builder.append('(').append(toString(x)).append(',').append(toString(y)).append(')');
    }
    builder.append('>');
    return builder.toString();
  }

  public static void logRetryMessage(Logger logger, Task task, int retryCount, Exception e) {
    String message = "Will retry task: " + task + ". retryCount=" + retryCount;
    if (e instanceof ConcurrentModificationException) {
      // Don't print stack trace in this case.
      logger.log(Level.INFO, message + " " + e.getMessage());
    } else {
      logger.log(Level.INFO, message, e);
    }
  }
}
