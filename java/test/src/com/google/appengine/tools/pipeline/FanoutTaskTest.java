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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.appengine.repackaged.com.google.common.collect.Sets;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.FinalizeJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleSlotFilledTask;
import com.google.appengine.tools.pipeline.impl.tasks.ObjRefTask;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import junit.framework.TestCase;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class FanoutTaskTest extends TestCase {

  private LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    System
        .setProperty("com.google.appengine.api.pipeline.use-simple-guids-for-debugging", "true");
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testFanoutTask() throws Exception {
    // step 1. Build a list of tasks
    Key key = KeyFactory.createKey(JobRecord.DATA_STORE_KIND, "job1");
    RunJobTask runJobTask = new RunJobTask(key, null);
    key = KeyFactory.createKey(JobRecord.DATA_STORE_KIND, "job2");
    RunJobTask runJobTask2 = new RunJobTask(key, null);
    key = KeyFactory.createKey(JobRecord.DATA_STORE_KIND, "job3");
    FinalizeJobTask finalizeJobTask = new FinalizeJobTask(key, null);
    key = KeyFactory.createKey(Slot.DATA_STORE_KIND, "slot1");
    HandleSlotFilledTask hsfTask = new HandleSlotFilledTask(key, null);
    LinkedList<Task> taskList =
        Lists.<Task> newLinkedList(runJobTask, runJobTask2, finalizeJobTask, hsfTask);
    // step 2. Build a fanout task that represents the tasks in the list
    FanoutTask fanoutTask = new FanoutTask(taskList);
    // step 3. Extract the properties from the fanout task
    Properties properties = fanoutTask.toProperties();
    // step 3. Reconstruct a fanout task from the properties
    fanoutTask = new FanoutTask(properties);
    // step 4. Extract a list of tasks from the reconstructed fanout task.
    List<Task> tasks = fanoutTask.getTasks();
    // step 5. Check that the reconstructed tasks are the same as the original.
    assertEquals(4, tasks.size());
    Set<String> expectedObjectNames = Sets.newHashSet("job1", "job2", "job3", "slot1");
    for (Task task : tasks) {
      checkTask(expectedObjectNames, task);
    }
  }

  private void checkTask(Set<String> expectedObjectNames, Task task) {
    assertNotNull(task.getName());
    assertTrue(expectedObjectNames.remove(((ObjRefTask) task).getKey().getName()));
  }

}
