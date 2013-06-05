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
 * An abstract represention of an input to our output from a job.
 * <p>
 * There are two types of{@code Values}: {@link FutureValue FutureValues} and
 * {@link ImmediateValue ImmediateValues}. {@link FutureValue FutureValues}
 * represent a value slot that will be filled in the future when some Job runs
 * and produces output.
 * <p>
 * There is also a special type of {@code FutureValue} called a
 * {@link PromisedValue}. These represent a value slot that will be filled in
 * not by a Job, but rather by some asynchronous, out-of-process entity, such as
 * a human filling in a web form, or the callback hook of a MapReduce.
 * <p>
 * An {@link ImmediateValue} represents a value that is known now. An instance
 * of {@code ImmediateValue} wraps a a concrete value such as an {@code Integer}
 * or a {@link String}) and may be obtained by invoking the method
 * {@link Job#immediate(Object)}.
 * <p>
 * In the framework, {@code Values} are used in several places:
 * <ul>
 * <li>They are used as arguments to the {@code futureCall()} family of methods.
 * Here both {@code FutureValues} and {@code ImmediateValues} may be used and
 * the {@code Value} represents the input to a Job.
 * <li> {@code FutureValues} are given as the output from the {@code
 * futureCall()} family of methods. Here they represent the output from a Job.
 * <li> {@code Values} of either type are the return value from a Job's {@code
 * run()} method. If a Job returns an {@code ImmediateValue} then the Job is
 * called an <em>immediate</em> Job. Such a Job computes its final output during
 * its {@code run()} method. If a Job returns a {@code FutureValue} then the Job
 * is a <em>generator</em> Job. Such a job generates a child Job graph during
 * its {@code run()} method and the Job's final output will be computed by one
 * of the child Jobs.
 * </ul>
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 * @param <E> The underlying type represented by this <code>Value</code>
 */
public interface Value<E> {
}
