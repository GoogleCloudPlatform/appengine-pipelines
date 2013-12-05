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

package com.google.appengine.tools.pipeline.demo;

import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A letter count pipeline job example.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class LetterCountExample {

  /**
   * Letter counter job.
   */
  public static class LetterCounter extends Job1<SortedMap<Character, Integer>, String> {

    private static final long serialVersionUID = -42446767578960124L;

    @Override
    public Value<SortedMap<Character, Integer>> run(String text) {
      String[] words = text.split("[^a-zA-Z]");
      List<FutureValue<SortedMap<Character, Integer>>> countsForEachWord = new LinkedList<>();
      for (String word : words) {
        countsForEachWord.add(futureCall(new SingleWordCounterJob(), immediate(word)));
      }
      return futureCall(new CountCombinerJob(), futureList(countsForEachWord));
    }
  }

  /**
   * Character counter per word.
   */
  public static class SingleWordCounterJob extends Job1<SortedMap<Character, Integer>, String> {

    private static final long serialVersionUID = 3257449383642363412L;

    @Override
    public Value<SortedMap<Character, Integer>> run(String word) {
      return immediate(countLetters(word));
    }
  }

  public static SortedMap<Character, Integer> countLetters(String text) {
    SortedMap<Character, Integer> charMap = new TreeMap<>();
    for (char c : text.toCharArray()) {
      incrementCount(c, 1, charMap);
    }
    return charMap;
  }

  /**
   * Combiner for the letter counting.
   */
  public static class CountCombinerJob extends
      Job1<SortedMap<Character, Integer>, List<SortedMap<Character, Integer>>> {

    private static final long serialVersionUID = -142472702334430476L;

    @Override
    public Value<SortedMap<Character, Integer>> run(
        List<SortedMap<Character, Integer>> listOfMaps) {
      SortedMap<Character, Integer> totalMap = new TreeMap<>();
      for (SortedMap<Character, Integer> charMap : listOfMaps) {
        for (Entry<Character, Integer> pair : charMap.entrySet()) {
          incrementCount(pair.getKey(), pair.getValue(), totalMap);
        }
      }
      return immediate(totalMap);
    }
  }

  private static void incrementCount(char c, int increment, Map<Character, Integer> charMap) {
    Integer countInteger = charMap.get(c);
    int count = (null == countInteger ? 0 : countInteger) + increment;
    charMap.put(c, count);
  }

  public static void main(String[] args) {
    String text = "ab cd";
    String regex = "[^a-z,A-Z]";
    String[] words = text.split(regex);
    for (String word : words) {
      System.out.println("[" + word + "]");
    }
  }
}
