package com.google.appengine.tools.pipeline.demo;

import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.util.Pair;

public class GCDExample {

  /**
   * Performs the Euclidean algorithm on a pair of non-negative integers (a,b).
   * Returns GCD(a,b)
   */
  public static class GCDJob extends Job2<Integer, Integer, Integer> {
    @Override
    public Value<Integer> run(Integer a, Integer b) {
      checkPositive(a, "a");
      checkPositive(b, "b");
      FutureValue<Pair<Integer, Integer>> orderedPair =
          futureCall(new OrderIntsJob(), immediate(a), immediate(b));
      return futureCall(new EuclAlgJob(), orderedPair);
    }

    private void checkPositive(Integer x, String name) {
      if (null == x) {
        throw new IllegalArgumentException(name + " is null.");
      }
      if (x <= 0) {
        throw new IllegalArgumentException(name + " is not positive: " + x);
      }
    }
  }


  /**
   * Performs the Euclidean algorithm on a pair of non-negative integers (a,b)
   * assuming the work has already been done to assure that a<=b Returns the GCD
   * of (a,b)
   */
  public static class EuclAlgJob extends Job1<Integer, Pair<Integer, Integer>> {
    @Override
    public Value<Integer> run(Pair<Integer, Integer> intPair) {
      try {
        int a = intPair.first;
        int b = intPair.second;
        // Assume a<=b
        if (a == b) {
          return immediate(a);
        } else {
          // Else a<b
          FutureValue<Integer> difference = futureCall(new DiffJob(), immediate(b), immediate(a));
          FutureValue<Integer> gcd = futureCall(new GCDJob(), immediate(a), difference);
          return gcd;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }


  /**
   * Given two integers a and b returns the pair (a,b) if a<=b else returns the
   * pair (b,a).
   */
  public static class OrderIntsJob extends Job2<Pair<Integer, Integer>, Integer, Integer> {

    @Override
    public Value<Pair<Integer, Integer>> run(Integer a, Integer b) {
      Pair<Integer, Integer> pair;
      if (a < b) {
        return immediate(new Pair<Integer, Integer>(a, b));
      } else {
        return immediate(new Pair<Integer, Integer>(b, a));
      }
    }
  }

  /**
   * Given two integers b and a returns b - a.
   */
  public static class DiffJob extends Job2<Integer, Integer, Integer> {
    @Override
    public Value<Integer> run(Integer b, Integer a) {
      return immediate(b - a);
    }
  }

}
