// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.servlets;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class StaticContentHandler {
  private static Logger logger = Logger.getLogger(StaticContentHandler.class.getName());

  private static final int BUFFER_SIZE = 1024 * 2;
  private static final String UI_DIR = "ui/";

  private static final String[][] RESOURCES = { 
      {"status.html", "status.html", "text/html"},
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
      {"images/treeview-black-line.gif", "images/treeview-black-line.gif", "image/gif"},};

  private static class NameContentTypePair {
    public String fileName;
    public String contentType;
    public NameContentTypePair(String name, String type){
      this.fileName = name;
      this.contentType = type;
    }
  }

  private static final Map<String, NameContentTypePair> RESOURCE_MAP;

  static {
    RESOURCE_MAP = new ConcurrentHashMap<String, NameContentTypePair>(RESOURCES.length);
    for (String[] triple : RESOURCES) {
      String urlPath = triple[0];
      String fileName = triple[1];
      String contentType = triple[2];
      RESOURCE_MAP.put(urlPath, new NameContentTypePair(fileName, contentType));
    }
  }

  public static void doGet(HttpServletRequest req, HttpServletResponse resp, String path)
      throws ServletException {
    try {
      NameContentTypePair pair = RESOURCE_MAP.get(path);
      if (pair == null) {
        logger.warning("Resource not found: " + path);
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        resp.getWriter().write("Resource not found.");
        resp.setContentType("text/plain");
        return;
      }
      String localPath = UI_DIR + pair.fileName;
      String contentType = pair.contentType;
      resp.setContentType(contentType);
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.addHeader("Cache-Control", "public; max-age=300");
      InputStream in = StaticContentHandler.class.getResourceAsStream(localPath);
      ReadableByteChannel readChannel = Channels.newChannel(in);
      WritableByteChannel writeChannel = Channels.newChannel(resp.getOutputStream());
      ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
      while (true) {
        buffer.clear();
        if (-1 == readChannel.read(buffer)) {
          break;
        }
        buffer.flip();
        writeChannel.write(buffer);
      }
      writeChannel.close();
      readChannel.close();
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }

}
