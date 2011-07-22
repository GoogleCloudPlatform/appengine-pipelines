package com.google.appengine.tools.pipeline;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.appengine.tools.pipeline.demo.LetterCountExample;
import com.google.appengine.tools.pipeline.demo.LetterCountExample.LetterCounter;

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
    SortedMap<Character, Integer> counts = waitForJobToComplete(pipelineId);
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
