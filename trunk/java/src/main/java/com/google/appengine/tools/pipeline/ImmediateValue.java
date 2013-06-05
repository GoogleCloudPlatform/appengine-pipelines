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
 * An implementation of {@link Value} which simply wraps a concrete value.
 * <p>
 * There are two places in the framework where this class must be used. Firstly,
 * when invoking any of the {@code futureCall(job, argument...)} family of
 * methods, when an argument to {@code job} is known at the time {@code
 * futureCall()} is invoked, then this argument must be passed as an argument to
 * {@code futureCall()} as and {@code ImmediateValue}. For example the following
 * code might appear inside of the {@code run()} method of a Job. <blockquote>
 * 
 * <pre>
 * FutureValue&lt;String&gt; name = futureCall(new GetNameJob());
 * futureCall(new GreetingJob(), new ImmediateValue(&quot;Hello&quot;), name);
 * </pre>
 * 
 * </blockquote> The arguments to {@code futureCall()} following the {@code Job}
 * argument must be {@code Values}. Thus in the above code in order to indicate
 * that the String "Hello" should be passed as the first argument to {@code
 * GreetingJob}, the argument {@code new ImmediateValue("Hello")} is passed to
 * {@code futureCall()}. Notice that the second argument to {@code futureCall()}, 
 * {@code name}, is already a {@code Value}, namely a {@code FutureValue}, and
 * so it is passed directly.
 * <p>
 * As some syntactic sugar the framework provides the method
 * {@link Job#immediate(Object)}. Thus the above code could instead be written
 * as follows: blockquote>
 * 
 * <pre>
 * FutureValue&lt;String&gt; name = futureCall(new GetNameJob());
 * futureCall(new GreetingJob(), immediate(&quot;Hello&quot;), name);
 * </pre>
 * 
 * </blockquote>
 * <p>
 * The second place where an {@code ImmediateValue} may be used in the framework
 * is as a return value from a {@code run()} method. Thus the following code may
 * appear inside of the {@code run()} method of a Job. <blockquote>
 * 
 * <pre>
 * int x = calculateValue(y);
 * return immediate(x)
 * </pre>
 * 
 * </blockquote> Again, the return value from {@code run()} must be a {@code
 * Value} and so a concrete value such as {@code x} above must be wrapped in an
 * {@code ImmediateValue}.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 * @param <E> The type of the underlying value wrapped by this {@code
 *        ImmediateValue}
 */
public final class ImmediateValue<E> implements Value<E> {

  private E value;

  public ImmediateValue(E val) {
    this.value = val;
  }

  public E getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "Immediate[" + value + "]";
  }
}
