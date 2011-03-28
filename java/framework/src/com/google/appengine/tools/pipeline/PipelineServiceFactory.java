package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;

/**
 * A factory for obtaining instances of {@link PipelineService}
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class PipelineServiceFactory {
  private PipelineServiceFactory(){
  }
  public static PipelineService newPipelineService() {
    return new PipelineServiceImpl();
  }
}
