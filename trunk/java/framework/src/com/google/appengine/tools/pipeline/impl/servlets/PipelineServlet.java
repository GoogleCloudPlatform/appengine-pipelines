package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.pipeline.util.Pair;

import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class PipelineServlet extends HttpServlet {

  Logger logger = Logger.getLogger(PipelineServlet.class.getName());

  // This must match the URL in web.xml
  public static final String BASE_URL = "/_ah/pipeline/";

  private static enum RequestType {

    HANDLE_TASK(TaskHandler.PATH_COMPONENT), GET_JSON(JsonHandler.PATH_COMPONENT), HANDLE_STATIC("");

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
    if(null == requestType){
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
