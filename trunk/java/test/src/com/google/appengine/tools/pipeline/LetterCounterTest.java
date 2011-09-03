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

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.appengine.tools.pipeline.demo.LetterCountExample;
import com.google.appengine.tools.pipeline.demo.LetterCountExample.LetterCounter;

/**
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class LetterCounterTest extends PipelineTest {

  public void testLetterCounter() throws Exception {
    doLetterCounterTest("Only three words.");
    doLetterCounterTest("The quick brown fox jumps over the lazy dog.");
    doLetterCounterTest("The woods are lovely dark and deep. " + "But I have promises to keep. "
        + "And miles to go before I sleep.");
  }

  private void doLetterCounterTest(String text) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new LetterCounter(), text);
    @SuppressWarnings("unchecked")
    SortedMap<Character, Integer> counts =
        (SortedMap<Character, Integer>) waitForJobToComplete(pipelineId);
    SortedMap<Character, Integer> expectedCounts = LetterCountExample.countLetters(text);
    SortedMap<Character, Integer> expectedCountsLettersOnly = new TreeMap<Character, Integer>();
    for (Entry<Character, Integer> entry : expectedCounts.entrySet()) {
      if (Character.isLetter(entry.getKey())) {
        expectedCountsLettersOnly.put(entry.getKey(), entry.getValue());
      }
    }
    assertEquals(expectedCountsLettersOnly, counts);
  }
}
