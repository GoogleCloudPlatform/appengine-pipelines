package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.appengine.api.datastore.Key;

import java.util.Properties;

public class RunJobTask extends ObjRefTask {

  public RunJobTask(Key jobKey, String name) {
    super(Type.RUN_JOB, name, jobKey);
  }

  public RunJobTask(Properties properties) {
    super(Type.RUN_JOB, properties);
  }

  public Key getJobKey() {
    return key;
  }

}
