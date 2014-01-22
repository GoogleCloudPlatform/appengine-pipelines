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

package com.google.appengine.tools.pipeline.impl.servlets;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class StaticContentHandler {
  private static Logger logger = Logger.getLogger(StaticContentHandler.class.getName());

  private static final int BUFFER_SIZE = 1024 * 2;
  private static final String UI_DIR = "ui/";
  // This is where the ui files end up if the library is built internally at Google:
  private static final String INTERNAL_BUILD_UI_DIR =
      "/third_party/py/appengine_pipeline/src/pipeline/ui/";

  private static final String[][] RESOURCES = {
      {"list", "root_list.html", "text/html"},
      {"list.css", "root_list.css", "text/css"},
      {"list.js", "root_list.js", "text/javascript"},
      {"status.html", "status.html", "text/html"}, // Legacy
      {"status", "status.html", "text/html"},
      {"status.css", "status.css", "text/css"},
      {"status.js", "status.js", "text/javascript"},
      {"common.js", "common.js", "text/javascript"},
      {"common.css", "common.css", "text/css"},
      {"jquery-1.4.2.min.js", "jquery-1.4.2.min.js", "text/javascript"},
      {"jquery.treeview.min.js", "jquery.treeview.min.js", "text/javascript"},
      {"jquery.cookie.js", "jquery.cookie.js", "text/javascript"},
      {"jquery.timeago.js", "jquery.timeago.js", "text/javascript"},
      {"jquery.ba-hashchange.min.js", "jquery.ba-hashchange.min.js", "text/javascript"},
      {"jquery.json.min.js", "jquery.json.min.js", "text/javascript"},
      {"jquery.treeview.css", "jquery.treeview.css", "text/css"},
      {"images/treeview-default.gif", "images/treeview-default.gif", "image/gif"},
      {"images/treeview-default-line.gif", "images/treeview-default-line.gif", "image/gif"},
      {"images/treeview-black.gif", "images/treeview-black.gif", "image/gif"},
      {"images/treeview-black-line.gif", "images/treeview-black-line.gif", "image/gif"}};

  private static class NameContentTypePair {

    public final String fileName;
    public final String contentType;

    public NameContentTypePair(String name, String type) {
      fileName = name;
      contentType = type;
    }
  }

  private static final Map<String, NameContentTypePair> RESOURCE_MAP;

  static {
    Map<String, NameContentTypePair> map = new HashMap<>(RESOURCES.length + 1, 1);
    for (String[] triple : RESOURCES) {
      String urlPath = triple[0];
      String fileName = triple[1];
      String contentType = triple[2];
      map.put(urlPath, new NameContentTypePair(fileName, contentType));
    }

    RESOURCE_MAP = Collections.unmodifiableMap(map);
  }

  public static void doGet(HttpServletResponse resp, String path) throws ServletException {
    try {
      NameContentTypePair pair = RESOURCE_MAP.get(path);
      if (pair == null) {
        logger.warning("Resource not found: " + path);
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        resp.getWriter().write("Resource not found.");
        resp.setContentType("text/plain");
        return;
      }
      String contentType = pair.contentType;
      resp.setContentType(contentType);
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.addHeader("Cache-Control", "public; max-age=300");
      try (InputStream in = getResourceAsStream(pair.fileName);
          ReadableByteChannel readChannel = Channels.newChannel(in);
          WritableByteChannel writeChannel = Channels.newChannel(resp.getOutputStream())) {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        while (true) {
          buffer.clear();
          if (-1 == readChannel.read(buffer)) {
            break;
          }
          buffer.flip();
          writeChannel.write(buffer);
        }
      }
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }

  // Visible for testing
  public static InputStream getResourceAsStream(String fileName) throws FileNotFoundException {
    String localPath = UI_DIR + fileName;
    String altLocalPath = INTERNAL_BUILD_UI_DIR + fileName;
    InputStream in = StaticContentHandler.class.getResourceAsStream(localPath);
    if (in == null) {
      in = StaticContentHandler.class.getResourceAsStream(altLocalPath);
    }
    if (in == null) {
      throw new FileNotFoundException(localPath + " <or> " + altLocalPath);
    }
    return in;
  }
}
