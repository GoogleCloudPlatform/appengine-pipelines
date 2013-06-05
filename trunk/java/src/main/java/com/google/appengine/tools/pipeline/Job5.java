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
 * takes five input parameter.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 * @param <T> The type of the job's output.
 * @param <T1> The type of the job's first input.
 * @param <T2> The type of the job's second input.
 * @param <T3> The type of the job's third input.
 * @param <T4> The type of the job's fourth input.
 * @param <T5> The type of the job's fifth input.
 */
public abstract class Job5<T, T1, T2, T3, T4, T5> extends Job<T> {

  private static final long serialVersionUID = 877904225346405931L;

  /**
   * Users must define this method in their job class.
   * 
   * @throws Exception in case of job failure.
   */
  public abstract Value<T> run(T1 param1, T2 param2, T3 param3, T4 param4, T5 param5)
      throws Exception;
}
