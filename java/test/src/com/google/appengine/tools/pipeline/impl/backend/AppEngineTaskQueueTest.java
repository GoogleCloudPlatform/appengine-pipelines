package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

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
    helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), taskQueueConfig);
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
    List<TaskHandle> handles =
        queue.addToQueue(Collections.singletonList(queue.toTaskOptions(task)));

    assertEquals(1, handles.size());
    assertEquals(task.getName(), handles.get(0).getName());

    handles = queue.addToQueue(Collections.singletonList(queue.toTaskOptions(task)));
    assertEquals(0, handles.size());
  }

  public void testEnqueueBatchTasks() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    List<Task> tasks = new ArrayList<Task>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    List<TaskOptions> taskOptions =
        new ArrayList<TaskOptions>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      Task task = createTask();
      tasks.add(task);
      taskOptions.add(queue.toTaskOptions(task));
    }
    List<TaskHandle> handles = queue.addToQueue(taskOptions);

    assertEquals(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE, handles.size());
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getName());
    }

    handles = queue.addToQueue(taskOptions);
    assertEquals(0, handles.size());
  }

  public void testEnqueueBatchTwoStages() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    List<Task> tasks = new ArrayList<Task>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    List<TaskOptions> taskOptions =
        new ArrayList<TaskOptions>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      Task task = createTask();
      tasks.add(task);
      taskOptions.add(queue.toTaskOptions(task));
    }
    
    int firstBatchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE / 2;
    List<TaskHandle> handles = queue.addToQueue(taskOptions.subList(0, firstBatchSize));

    assertEquals(firstBatchSize, handles.size());
    for (int i = 0; i < firstBatchSize; i++) {
      assertEquals(tasks.get(i).getName(), handles.get(i).getName());
    }

    handles = queue.addToQueue(taskOptions);

    int expected = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE - firstBatchSize;
    assertEquals(expected, handles.size());
    for (int i = 0; i < expected; i++) {
      assertEquals(tasks.get(firstBatchSize + i).getName(), handles.get(i).getName());
    }
  }

  public void testEnqueueBatchTwoStagesShuffled() {
    AppEngineTaskQueue queue = new AppEngineTaskQueue();
    TreeSet<String> taskNames = new TreeSet<String>();
    
    List<TaskOptions> taskOptions =
        new ArrayList<TaskOptions>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
    
    for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
      Task task = createTask();
      taskNames.add(task.getName());
      taskOptions.add(queue.toTaskOptions(task));
    }
    
    int firstBatchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE / 2;
    List<TaskHandle> handles = queue.addToQueue(taskOptions.subList(0, firstBatchSize));


    assertEquals(firstBatchSize, handles.size());
    TreeSet<String> seenTaskNames = new TreeSet<String>();
    for (int i = 0; i < firstBatchSize; i++) {
      String name = handles.get(i).getName();
      assertTrue(taskNames.contains(name));
      assertTrue(seenTaskNames.add(name));
    }
    Collections.shuffle(taskOptions, new Random(0));
    handles = queue.addToQueue(taskOptions);

    int expected = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE - firstBatchSize;
    assertEquals(expected, handles.size());
    for (int i = 0; i < expected; i++) {
      String name = handles.get(i).getName();
      assertTrue(taskNames.contains(name));
      assertTrue(seenTaskNames.add(name));
    }
  }

  private Task createTask() {
    String name = GUIDGenerator.nextGUID();
    Key key = KeyFactory.createKey("testType", name);
    Task task = new RunJobTask(key);
    return task;
  }

}
