// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline.demo;

import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.Job3;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;

/**
 * This class contains the example Pipelines from the User Guide
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class UserGuideExamples {

  public static class ComplexJob extends Job3<Integer, Integer, Integer, Integer> {
    @Override
    public Value<Integer> run(Integer x, Integer y, Integer z) {
      DiffJob diffJob = new DiffJob();
      MultJob multJob = new MultJob();
      FutureValue<Integer> r = futureCall(diffJob, immediate(x), immediate(y));
      FutureValue<Integer> s = futureCall(diffJob, immediate(x), immediate(z));
      FutureValue<Integer> t = futureCall(multJob, r, s);
      FutureValue<Integer> u = futureCall(diffJob, t, immediate(2));
      return u;
    }
  }

  public static class DiffJob extends Job2<Integer, Integer, Integer> {
    @Override
    public Value<Integer> run(Integer a, Integer b) {
      return immediate(a - b);
    }
  }

  public static class MultJob extends Job2<Integer, Integer, Integer> {
    @Override
    public Value<Integer> run(Integer a, Integer b) {
      return immediate(a * b);
    }
  }
  
  public static class ExternalAgentJob extends Job1<Integer, String> {
    @Override
    public Value<Integer> run(String userEmail) {
      // Invoke ComplexJob on three promised values
      PromisedValue<Integer> x = newPromise(Integer.class);
      PromisedValue<Integer> y = newPromise(Integer.class);
      PromisedValue<Integer> z = newPromise(Integer.class);
      FutureValue<Integer> intermediate = futureCall(new ComplexJob(), x, y, z);
      
      // Kick off the process of retrieving the data from the external agent
      getIntFromUser("Please give 1st int", userEmail, x.getHandle());
      getIntFromUser("Please give 2nd int", userEmail, y.getHandle());
      getIntFromUser("Please give 3rd int", userEmail, z.getHandle());

      // Send the user the intermediate result and ask for one more integer
      FutureValue<Integer> oneMoreInt =
        futureCall(new PromptJob(), intermediate, immediate(userEmail));

      // Invoke MultJob on intermediate and oneMoreInt
      return futureCall(new MultJob(), intermediate, oneMoreInt);
    }

    public static void getIntFromUser(String prompt, String userEmail, 
                                                     String promiseHandle) {
      // 1. Send the user an e-mail containing the prompt.
      // 2. Ask user to submit one more integer on some web page.
      // 3. promiseHandle is a query string argument
      // 4. Handler for submit invokes submitPromisedValue(promiseHandle, value)
    }
  }

  public static class PromptJob extends Job2<Integer, Integer, String> {
    @Override
    public Value<Integer> run(Integer intermediate, String userEmail) {
      String prompt =
          "The intermediate result is " + intermediate + "." 
               + " Please give one more int";
      PromisedValue<Integer> oneMoreInt = newPromise(Integer.class);
      ExternalAgentJob.getIntFromUser(prompt, userEmail, oneMoreInt.getHandle());
      return oneMoreInt;
    }
  }
  
  public static class ExampleWaitForJob extends Job0<Void> {
    @Override
    public Value<Void> run() {
      FutureValue<Void> a = futureCall(new JobA());
      futureCall(new JobB(), waitFor(a));
      return null;
    }
  }
  
  public static class JobA extends Job0<Void> {
    @Override
    public Value<Void> run() {
      //Do something...
      return null;
    }
  }
  
  public static class JobB extends Job0<Void> {
    @Override
    public Value<Void> run() {
      // Do something...
      return null;
    }
  }

}
