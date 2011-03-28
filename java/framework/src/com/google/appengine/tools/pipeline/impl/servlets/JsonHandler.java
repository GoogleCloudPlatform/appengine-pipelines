// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author rudominer@google.com (Mitch Rudominer
 */
public class JsonHandler {

  public static final String PATH_COMPONENT = "rpc/tree";
  private static final String ROOT_PIPELINE_ID = "root_pipeline_id";

  public static void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException {

    String rootJobHandle = req.getParameter(ROOT_PIPELINE_ID);
    if (null == rootJobHandle) {
      throw new ServletException(ROOT_PIPELINE_ID + " parameter not found.");
    }
    PipelineObjects pipelineObjects = PipelineManager.queryFullPipeline(rootJobHandle);
    JsonGenerator generator = new JsonGenerator(pipelineObjects);
    try {
      resp.getWriter().write(generator.getJson());
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }
}
