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

import com.google.appengine.tools.pipeline.util.Pair;

import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that handles all requests for the Pipeline framework.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
@SuppressWarnings("serial")
public class PipelineServlet extends HttpServlet {

  Logger logger = Logger.getLogger(PipelineServlet.class.getName());

  // This must match the URL in web.xml
  public static final String BASE_URL = "/_ah/pipeline/";

  private static enum RequestType {

    HANDLE_TASK(TaskHandler.PATH_COMPONENT), GET_JSON(JsonHandler.PATH_COMPONENT), HANDLE_STATIC(
        "");

    private String pathComponent;

    private RequestType(String pathComponent) {
      this.pathComponent = pathComponent;
    }

    public boolean matches(String path) {
      return pathComponent.equals(path);
    }
  }

  private Pair<String, RequestType> parseRequestType(HttpServletRequest req) {
    String requestURI = req.getRequestURI();
    String path = requestURI.substring(BASE_URL.length());
    RequestType requestType = null;
    for (RequestType rt : RequestType.values()) {
      if (rt.matches(path)) {
        requestType = rt;
        break;
      }
    }
    if (null == requestType) {
      requestType = RequestType.HANDLE_STATIC;
    }
    return new Pair<String, RequestType>(path, requestType);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
    Pair<String, RequestType> pair = parseRequestType(req);
    RequestType requestType = pair.second;
    String path = pair.first;
    switch (requestType) {
      case HANDLE_TASK:
        TaskHandler.doPost(req, resp);
        break;
      case GET_JSON:
        JsonHandler.doGet(req, resp);
        break;
      case HANDLE_STATIC:
        StaticContentHandler.doGet(req, resp, path);
        break;
      default:
        throw new ServletException("Unknown request type: " + requestType);
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
    Pair<String, RequestType> pair = parseRequestType(req);
    RequestType requestType = pair.second;
    String path = pair.first;
    switch (requestType) {
      case HANDLE_TASK:
        TaskHandler.doPost(req, resp);
        break;
      case GET_JSON:
        JsonHandler.doGet(req, resp);
        break;
      case HANDLE_STATIC:
        StaticContentHandler.doGet(req, resp, path);
        break;
      default:
        throw new ServletException("Unknown request type: " + requestType);
    }
  }

}
