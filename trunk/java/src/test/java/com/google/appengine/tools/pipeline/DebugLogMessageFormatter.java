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

package com.google.appengine.tools.pipeline;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class DebugLogMessageFormatter extends Formatter {

  /*
   * (non-Javadoc)
   * 
   * @see java.util.logging.Formatter#format(java.util.logging.LogRecord)
   */
  @Override
  public String format(LogRecord record) {
    StringBuilder builder = new StringBuilder();
    builder.append(record.getThreadID() + " : " + record.getMessage());
    Throwable e = record.getThrown();
    if (null != e) {
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
