package com.google.appengine.tools.pipeline.impl.util;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class GUIDGenerator {
    private static AtomicInteger counter = new AtomicInteger();
	
    private static final String USE_SIMPLE_GUIDS_FOR_DEBUGGING = "com.google.appengine.api.pipeline.use-simple-guids-for-debugging";
    
    public static synchronized String nextGUID(){
      if (Boolean.getBoolean(USE_SIMPLE_GUIDS_FOR_DEBUGGING)){
        return ""+ counter.getAndIncrement();
      }
	  UUID uuid = UUID.randomUUID();
	  return uuid.toString();
	}
    
    public static void main(String[] args){
      for(int i=0; i<10;i++) {
        System.out.println(nextGUID());
      }
    }
}
