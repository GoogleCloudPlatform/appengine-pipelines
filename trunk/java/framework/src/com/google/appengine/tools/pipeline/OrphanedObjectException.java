// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class OrphanedObjectException extends Exception {
  private static final long serialVersionUID = 149320669328554627L;

  public OrphanedObjectException(String key) {
    super(key);
  }

  public OrphanedObjectException(String key, Throwable cause) {
    super(key, cause);
  }
}

