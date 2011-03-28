// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.util;

import java.io.Serializable;

/**
 * An ordered pair.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class Pair<S, T> implements Serializable {
  public S first;
  public T second;

  public Pair(S first, T second) {
    this.first = first;
    this.second = second;
  }

  public S getFirst() {
    return first;
  }

  public T getSecond() {
    return second;
  }
  
  @Override
  public String toString(){
    return "Pair(" + first + ", " + second + ")";
  }
}
