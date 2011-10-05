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

/**
 * An Exception that indicates that the framework cannot find an object with a
 * specified identifier.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class NoSuchObjectException extends Exception {
  public NoSuchObjectException(String key) {
    super(key);
  }

  public NoSuchObjectException(String key, Throwable cause) {
    super(key, cause);
  }
}