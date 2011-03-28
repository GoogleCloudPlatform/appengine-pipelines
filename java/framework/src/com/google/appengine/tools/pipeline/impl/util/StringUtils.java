package com.google.appengine.tools.pipeline.impl.util;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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
