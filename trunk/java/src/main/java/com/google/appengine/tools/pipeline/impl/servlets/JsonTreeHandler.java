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

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class JsonTreeHandler {

  public static final String PATH_COMPONENT = "rpc/tree";
  private static final String ROOT_PIPELINE_ID = "root_pipeline_id";

  public static void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException {

    String rootJobHandle = req.getParameter(ROOT_PIPELINE_ID);
    if (null == rootJobHandle) {
      throw new ServletException(ROOT_PIPELINE_ID + " parameter not found.");
    }
    try {
      PipelineObjects pipelineObjects;
      try {
        pipelineObjects = PipelineManager.queryFullPipeline(rootJobHandle);
      } catch (IllegalArgumentException ex) {
        resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        return;
      }
      String asJson = JsonGenerator.pipelineObjectsToJson(pipelineObjects, rootJobHandle);
      resp.getWriter().write(asJson);
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }
}
