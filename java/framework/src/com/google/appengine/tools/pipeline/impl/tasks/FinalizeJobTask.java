package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.appengine.api.datastore.Key;

import java.util.Properties;

public class FinalizeJobTask extends ObjRefTask {
  
  public FinalizeJobTask(Key jobKey, String name) {
    super(Type.FINALIZE_JOB, name, jobKey);
  }

  public FinalizeJobTask(Properties properties) {
    super(Type.FINALIZE_JOB, properties);
  }

  public Key getJobKey() {
    return key;
  }

}
