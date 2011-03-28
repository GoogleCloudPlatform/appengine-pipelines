// Copyright 2011 Google Inc. All Rights Reserved.

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
 * @author rudominer@google.com (Your Name Here)
 *
 */
public class LetterCountExample {

  public static class LetterCounter extends Job1<SortedMap<Character, Integer>, String> {
    @Override
    public Value<SortedMap<Character, Integer>> run(String text) {
      String[] words = text.split("[^a-z,A-Z]");
      List<FutureValue<SortedMap<Character, Integer>>> countsForEachWord =
          new LinkedList<FutureValue<SortedMap<Character, Integer>>>();
      for (String word : words) {
        countsForEachWord.add(futureCall(new SingleWordCounterJob(), immediate(word)));
      }
      return futureCall(new CountCombinerJob(), futureList(countsForEachWord));
    }
  }

  public static class SingleWordCounterJob extends Job1<SortedMap<Character, Integer>, String> {
    @Override
    public Value<SortedMap<Character, Integer>> run(String word) {
      return immediate(countLetters(word));
    }
  }
  
  public static SortedMap<Character, Integer> countLetters(String text){
    SortedMap<Character, Integer> charMap = new TreeMap<Character, Integer>();
    for (char c : text.toCharArray()) {
      incrementCount(c, 1, charMap);
    }
    return charMap;
  }

  public static class CountCombinerJob extends Job1<
      SortedMap<Character, Integer>, List<SortedMap<Character, Integer>>> {
    @Override
    public Value<SortedMap<Character, Integer>> run(List<SortedMap<Character, Integer>> listOfMaps) {
      SortedMap<Character, Integer> totalMap = new TreeMap<Character, Integer>();
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
  
  public static void main(String[] args){
    String text = "ab cd";
    String regex = "[^a-z,A-Z]";
    String[] words = text.split(regex);
    for(String word : words){
      System.out.println("[" + word + "]");
    }
  }
}
