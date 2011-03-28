package com.google.appengine.tools.pipeline.impl;

@SuppressWarnings("serial")
public class RunJobException extends Exception {
	
	public RunJobException(String reason, Throwable cause){
		super(reason, cause);
	}
	
	public RunJobException(Throwable cause){
		super(cause);
	}
}
