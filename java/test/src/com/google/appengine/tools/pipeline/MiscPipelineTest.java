package com.google.appengine.tools.pipeline;


public class MiscPipelineTest extends PipelineTest {
  
  public void testImmediateChild() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new Returns5FromChildJob());
    Integer five = waitForJobToComplete(pipelineId);
    assertEquals(5, five.intValue());
  }
  
  private static class Returns5FromChildJob extends Job0<Integer> {
    public Value<Integer> run() {
      return futureCall(new IdentityJob(), immediate(5));
    }
  }
  
  private static class IdentityJob extends Job1<Integer,Integer>{
    @Override
    public Value<Integer> run(Integer param1) {
      return immediate(param1);
    }
  }
  
}
