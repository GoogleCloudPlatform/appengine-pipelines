package com.google.appengine.tools.pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Note the difference between testFutureList() and testReturnFutureList(). In
 * testFutureList() the call to futureList() happens in the parent job,
 * SumsListJob. In testReturnFutureList() the call to futureList() happens in
 * the child job, ReturnsListJob.
 */
public class FutureListTest extends PipelineTest {

  public void testFutureList() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new SumsListJob1());
    Integer sum = waitForJobToComplete(pipelineId);
    assertEquals(21, sum.intValue());
  }

  public void testReturnFutureList() throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new SumsListJob2());
    Integer sum = waitForJobToComplete(pipelineId);
    assertEquals(21, sum.intValue());
  }

  /**
   * In this job, the call to futureList() happens not in a child job but in
   * this job itself. This means that the FutureList is not the return value of
   * any job.
   * 
   * @author rudominer
   * 
   */
  private static class SumsListJob1 extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      Returns5Job returns5Job = new Returns5Job();
      SumJob sumJob = new SumJob();
      List<Value<Integer>> valueList = new ArrayList<Value<Integer>>(4);
      valueList.add(futureCall(returns5Job));
      valueList.add(immediate(7));
      valueList.add(futureCall(returns5Job));
      valueList.add(immediate(4));
      return futureCall(sumJob, futureList(valueList));
    }
  };

  /**
   * In this job, the call to futureList() happens in a child job, ReturnsList
   * job. This means that the FutureList is the return value of the child job
   */
  private static class SumsListJob2 extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      return futureCall(new SumJob(), futureCall(new ReturnsListJob()));
    }
  };

  private static class ReturnsListJob extends Job0<List<Integer>> {
    @Override
    public Value<List<Integer>> run() {
      Returns5Job returns5Job = new Returns5Job();
      List<Value<Integer>> valueList = new ArrayList<Value<Integer>>(4);
      valueList.add(futureCall(returns5Job));
      valueList.add(immediate(7));
      valueList.add(futureCall(returns5Job));
      valueList.add(immediate(4));
      return futureList(valueList);
    }
  }

  private static class Returns5Job extends Job0<Integer> {
    @Override
    public Value<Integer> run() {
      return immediate(5);
    }
  }

  private static class SumJob extends Job1<Integer, List<Integer>> {
    @Override
    public Value<Integer> run(List<Integer> list) {
      int sum = 0;
      for (int x : list) {
        sum += x;
      }
      return immediate(sum);
    }
  };
};
