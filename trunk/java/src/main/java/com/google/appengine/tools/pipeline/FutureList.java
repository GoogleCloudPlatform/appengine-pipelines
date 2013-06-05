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

import java.util.List;

/**
 * A converter from a {@code List} of {@code Values} to a {@code Value<List>}.
 * <p>
 * Use of this class makes it possible to take the results of an arbitrary
 * number of invocations of {@code futureCall()} and collect the results
 * together into a single {@code Value} that may be passed to another invocation
 * of {@code futureCall()}. For example:
 * 
 * <pre>
 * <code>
 * List&lt;FutureValue&lt;Integer&gt; listOfFutures = 
 *   new LinkedList&lt;FutureValue&lt;Integer&gt;&gt;();
 * for(int i = 0; i < n; i++) {
 *   listOfFutures.add(futureCall(new MyJob(), immediate(i), immediate(n));
 * }
 * FutureList<Integer> futureList = new FutureList(listOfFutures);
 * futureCall(new MyCombinerJob(), futureList);
 * </code>
 * </pre>
 * 
 * The invocation of {@code futureCall(new MyCombinerJob(), futureList)} above
 * instructs the framework that after the value slots corresponding to each of
 * the {@code FutureValues} in {@code listOfFutures} have been filled, the
 * {@code run()} method of {@code MyCombinerJob} should be invoked with a single
 * argument of type {@code List<E>} in the argument position corresponding to
 * {@code futureList} and containing the values that filled the slots, in the
 * order corresponding to {@code listOfFutures}.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 * @param <E> The type of object stored in the list
 */
public final class FutureList<E> implements Value<List<E>> {
  private List<? extends Value<E>> listOfValues;

  /**
   * Constructs a {@code FutureList} from a {@code List} of {@code Values}.
   * 
   * @param listOfValues a {@code List} of {@code Values}. This object takes
   *        ownership of the list. The client should not continue to hold a
   *        reference to it.
   */
  public FutureList(List<? extends Value<E>> listOfValues) {
    this.listOfValues = listOfValues;
    if (null == listOfValues) {
      throw new IllegalArgumentException("lisOfValues is null");
    }
  }

  /**
   * This method is used by the framework to retrieve the list of values.
   * User-code should not invoke this method.
   */
  public List<? extends Value<E>> getListOfValues() {
    return listOfValues;
  }
}
