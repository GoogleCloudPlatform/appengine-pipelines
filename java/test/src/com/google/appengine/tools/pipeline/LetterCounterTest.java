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

import com.google.appengine.tools.pipeline.demo.LetterCountExample;
import com.google.appengine.tools.pipeline.demo.LetterCountExample.LetterCounter;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

/**
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class LetterCounterTest extends PipelineTest {

  private static final String SSB =
      "Oh, say, can you see, by the dawn's early light, \n"
          + "What so proudly we hailed at the twilight's last gleaming? \n"
          + "Whose broad stripes and bright stars, thru the perilous fight, \n"
          + "O'er the ramparts we watched, were so gallantly streaming? \n"
          + "And the rockets' red glare, the bombs bursting in air, \n"
          + "Gave proof through the night that our flag was still there. \n"
          + "O say, does that star-spangled banner yet wave \n"
          + "O'er the land of the free and the home of the brave?\n"
          + "On the shore dimly seen through the mists of the deep, \n"
          + "Where the foe's haughty host in dread silence reposes, \n"
          + "What is that which the breeze, o'er the towering steep, \n"
          + "As it fitfully blows, half conceals, half discloses? \n"
          + "Now it catches the gleam of the morning's first beam, \n"
          + "In full glory reflected, now shines on the stream: \n"
          + "Tis the star-spangled banner: O, long may it wave \n"
          + "O'er the land of the free and the home of the brave!     \n"
          + "And where is that band who so vauntingly swore \n"
          + "That the havoc of war and the battle's confusion \n"
          + "A home and a country should leave us no more? \n"
          + "Their blood has washed out their foul footsteps' pollution. \n"
          + "No refuge could save the hireling and slave \n"
          + "From the terror of flight or the gloom of the grave: \n"
          + "And the star-spangled banner in triumph doth wave \n"
          + "O'er the land of the free and the home of the brave. \n"
          + "O, thus be it ever when freemen shall stand, \n"
          + "Between their loved home and the war's desolation! \n"
          + "Blest with victory and peace, may the heav'n-rescued land \n"
          + "Praise the Power that hath made and preserved us a nation! \n"
          + "Then conquer we must, when our cause it is just, \n"
          + "And this be our motto: \"In God is our trust\" \n"
          + "And the star-spangled banner in triumph shall wave \n"
          + "O'er the land of the free and the home of the brave!";

  public void testLetterCounter3() throws Exception {
    doLetterCounterTest("Only three words.");
  }

  public void testLetterCounter4() throws Exception {
    doLetterCounterTest("Only four short words.");
  }

  public void testLetterCounter5() throws Exception {
    doLetterCounterTest("Only five pretty short words.");
  }

  public void testLetterCounter6() throws Exception {
    doLetterCounterTest("Only six pretty short words total.");
  }

  public void testLetterCounterBig() throws Exception {
    doLetterCounterTest("The woods are lovely dark and deep. "
        + "But I have promises to keep. And miles to go before I sleep.");
  }
  
  
  public void testLetterCounterHuge() throws Exception {
    doLetterCounterTest(SSB);
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
