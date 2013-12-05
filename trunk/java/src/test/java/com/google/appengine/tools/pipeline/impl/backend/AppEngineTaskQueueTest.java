package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.LocalModulesServiceTestConfig;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author tkaitchuck
 */
public class AppEngineTaskQueueTest extends TestCase {

  private LocalServiceTestHelper helper;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setDisableAutoTaskExecution(true);
    taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
    helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), taskQueueConfig,
        new LocalModulesServiceTestConfig());
    helper.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testEnqueueSingleTask() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    Task task = createTask();
    List<TaskHandle> handles = queue.addToQueue(Collections.singletonList(task));

    assertEquals(1, handles.size());
    assertEquals(task.getName(), handles.get(0).getName());

    handles = queue.addToQueue(Collections.singletonList(task));
    assertEquals(0, handles.size());
  }

  public void testEnqueueBatchTasks() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    List<Task> tasks = new ArrayList<>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      Task task = createTask();
      tasks.add(task);
    }
    List<TaskHandle> handles = queue.addToQueue(tasks);
    assertEquals(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE, handles.size());
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getName());
    }

    handles = queue.addToQueue(tasks);
    assertEquals(0, handles.size());
  }

  public void testEnqueueLargeBatchTasks() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2 + 10;
    List<Task> tasks = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      Task task = createTask();
      tasks.add(task);
    }
    List<TaskHandle> handles = queue.addToQueue(tasks);
    assertEquals(tasks.size(), handles.size());
    for (int i = 0; i < tasks.size(); i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getName());
    }

    handles = queue.addToQueue(tasks);
    assertEquals(0, handles.size());
  }

  public void testEnqueueBatchTwoStages() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2;
    List<Task> tasks = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
      Task task = createTask();
      tasks.add(task);
    }

    int firstBatchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE;
    List<TaskHandle> handles = queue.addToQueue(tasks.subList(0, firstBatchSize));

    assertEquals(firstBatchSize, handles.size());
    for (int i = 0; i < firstBatchSize; i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getName());
    }

    handles = queue.addToQueue(tasks);

    // Duplicate is rejected (not counted) per batch.
    int expected = tasks.size() - firstBatchSize;
    assertEquals(expected, handles.size());
    for (int i = 0; i < expected; i++) {
      assertEquals(tasks.get(firstBatchSize + i).getName(), handles.get(i).getName());
    }
  }

  private Task createTask() {
    String name = GUIDGenerator.nextGUID();
    Key key = KeyFactory.createKey("testType", name);
    Task task = new RunJobTask(key, new QueueSettings().setModuleVersion("m1"));
    return task;
  }
}
