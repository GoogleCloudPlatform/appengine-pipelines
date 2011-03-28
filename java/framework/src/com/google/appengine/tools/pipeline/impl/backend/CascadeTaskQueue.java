package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.tasks.Task;

public interface CascadeTaskQueue {
	public void enqueue(Task task);
}
