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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.pipeline.util.Pair;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that handles all requests for the Pipeline framework.
 * Dispatches all requests to {@link TaskHandler}, {@link JsonHandler} or
 * {@link StaticContentHandler} as appropriate
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
@SuppressWarnings("serial")
public class PipelineServlet extends HttpServlet {

  // This must match the URL in web.xml
  public static final String BASE_URL = "/_ah/pipeline/";

  public static String makeViewerUrl(Key rootJobKey, Key jobKey) {
    // TODO(ohler): Make this a full copy&paste-ready URL including domain.
    return BASE_URL + "status.html?root=" + rootJobKey.getName()
        + "#pipeline-" + jobKey.getName();
  }

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
    String path = req.getPathInfo();
    if (path == null) {
      path = "";
    } else {
      // Take off the leading '/'
      path = path.substring(1);
    }
    RequestType requestType = RequestType.HANDLE_STATIC;
    for (RequestType rt : RequestType.values()) {
      if (rt.matches(path)) {
        requestType = rt;
        break;
      }
    }
    return new Pair<String, RequestType>(path, requestType);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
    doGet(req, resp);
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
