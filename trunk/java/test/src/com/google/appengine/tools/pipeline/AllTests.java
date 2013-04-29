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

package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueueTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class AllTests {

  public static Test suite() {
    TestSuite suite = new TestSuite(AllTests.class.getName());
    suite.addTestSuite(StaticContentHandlerTest.class);
    suite.addTestSuite(BarrierTest.class);
    suite.addTestSuite(FanoutTaskTest.class);
    suite.addTestSuite(MiscPipelineTest.class);
    suite.addTestSuite(UserGuideTest.class);
    suite.addTestSuite(OrphanedJobGraphTest.class);
    suite.addTestSuite(LetterCounterTest.class);
    suite.addTestSuite(FutureListTest.class);
    suite.addTestSuite(GCDTest.class);
    suite.addTestSuite(RetryTest.class);
    suite.addTestSuite(AppEngineTaskQueueTest.class);
    return suite;
  }

}
