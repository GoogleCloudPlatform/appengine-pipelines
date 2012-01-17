// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.impl.util;

/**
 * Some utilities to facilitate instrumenting the Pipeline code for enhanced
 * testability
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class TestUtils {

  private static final String FAIL_PROPERTY_PREFIX = TestUtils.class.getName() + ".failAt.";
  
  /**
   * Builds the name of the Java System property that should be set to
   * "true" in order for the method {@link #throwHereForTesting(String)}
   * to throw an exception when passed {@code breakId}.
   * 
   * @param breakId The id of the place to break. This value is found
   * within the instrumented code at places where we have instrumented it
   * to fail for tests.
   * 
   * @return The name of the Java System propert that should be set to "true".
   */
  public static String getFailureProperty(String breakId) {
    return FAIL_PROPERTY_PREFIX + "." + breakId;
  }

  /**
   * Throws a {@link RuntimeException} if an appropriate Java System property is
   * set to "true".
   * <p>
   * In order to test the consistency of our transactional model it is necessary
   * to instrument the Pipeline code to purposefully fail at certain points when
   * we request so.
   * 
   * @param breakId This method will throw a {@link RuntimeException} if the
   *        Java System property named x is "true" where x is the result of
   *        invoking {@link #getFailureProperty(String)} on {@code breakId}.
   */
  public static void throwHereForTesting(String breakId) {
    String propertyName = getFailureProperty(breakId);
    if (Boolean.parseBoolean(System.getProperty(propertyName))) {
      System.setProperty(propertyName, "false");
      throw new RuntimeException("Breaking for test at " + breakId);
    }
  }
}
