// Copyright 2010 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

/**
 * An Exception that indicates that the framework cannot find an object
 * with a specified identifier.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class NoSuchObjectException extends Exception {
  public NoSuchObjectException(String key){
    super(key);
  }
  public NoSuchObjectException(String key, Throwable cause){
    super(key, cause);
  }
}
