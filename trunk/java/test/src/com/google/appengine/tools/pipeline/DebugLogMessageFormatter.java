// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * @author rudominer@google.com (Your Name Here)
 *
 */
public class DebugLogMessageFormatter extends Formatter{

  /* (non-Javadoc)
   * @see java.util.logging.Formatter#format(java.util.logging.LogRecord)
   */
  @Override
  public String format(LogRecord record) {
    StringBuilder builder = new StringBuilder();
    builder.append(record.getThreadID() + " : " + record.getMessage());
    Throwable e = record.getThrown();
    if (null != e){
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      PrintWriter writer = new PrintWriter(stream);
      e.printStackTrace(writer);
      writer.flush();
      builder.append("\n");
      builder.append(new String(stream.toByteArray()));
    }
    builder.append("\n\n");
    return builder.toString();
  }

}
