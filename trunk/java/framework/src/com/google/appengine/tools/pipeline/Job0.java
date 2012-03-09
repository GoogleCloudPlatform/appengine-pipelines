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
 * A user's job class should subclass a specialization of this class if the job
 * does not take any input parameters.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 * @param <T> The type of the job's output.
 */
public abstract class Job0<T> extends Job<T> {

  private static final long serialVersionUID = 951850681860721384L;

  /**
   * Users must define this method in their job class.
   */
  public abstract Value<T> run();
}
