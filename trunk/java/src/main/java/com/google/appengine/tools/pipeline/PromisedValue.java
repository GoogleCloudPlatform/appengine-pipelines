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
 * An abstract representation of a value slot that will be filled in when some
 * asynchronous external agent supplies a value.
 * <p>
 * An instance of {@code PromisedValue} is obtained from within the {@code
 * run()} method of a Job via the methd {@link Job#newPromise(Class)}. Once
 * obtained in this way, an instance of {@code PromisedValue} may be used in the
 * same way that any {@link FutureValue} may be used. The {@link #getHandle()
 * handle} of a promised value is an opaque identifier that uniquely represents
 * the value slot to the framework. This handle should be passed to the external
 * agent who will use the handle to supply the promised value via the method
 * {@link PipelineService#submitPromisedValue(String, Object)}. For example the
 * following code might appear inside of the {@code run()} method of a Job.
 * <blockquote>
 * 
 * <pre>
 * PromisedValue<Integer> x = newPromise(Integer.class)
 * String xHandle = x.getHandle();
 * invokeExternalAgent(xHandle)
 * futureCall(new UsesIntegerJob(), x);
 * </pre>
 * 
 * </blockquote> The external agent will provide the promised integer value at
 * some point in the future by invoking {@code
 * pipelineService.acceptPromisedValue(handle, value)}, where {@code handle} is
 * a String equal to {@code xHandle} and {@code value} is some integer value.
 * The framework will then invoke the {@code run()} method of the {@code
 * UsesIntegerJob} passing in {@code value}.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 * @param <E> The type of the value represented by this {@code PromisedValue}
 */
public interface PromisedValue<E> extends FutureValue<E> {
  String getHandle();
}
