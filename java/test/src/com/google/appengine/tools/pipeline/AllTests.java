package com.google.appengine.tools.pipeline;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests {

  public static Test suite() {
    TestSuite suite = new TestSuite(AllTests.class.getName());
    suite.addTestSuite(BarrierTest.class);
    suite.addTestSuite(FanoutTaskTest.class);
    suite.addTestSuite(MiscPipelineTest.class);
    suite.addTestSuite(UserGuideTest.class);
    
    suite.addTestSuite(LetterCounterTest.class);
    suite.addTestSuite(FutureListTest.class);
    suite.addTestSuite(GCDTest.class);
    suite.addTestSuite(RetryTest.class);
    return suite;
  }

}
