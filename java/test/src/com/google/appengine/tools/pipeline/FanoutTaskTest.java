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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.pipeline.impl.model.FanoutTaskRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.FinalizeJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleSlotFilledTask;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import junit.framework.TestCase;

import java.util.List;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class FanoutTaskTest extends TestCase {

  private LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  private List<Task> listOfTasks;
  byte[] encodedBytes;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    System
        .setProperty("com.google.appengine.api.pipeline.use-simple-guids-for-debugging", "true");
    Key key = KeyFactory.createKey(JobRecord.DATA_STORE_KIND, "job1");
    RunJobTask runJobTask = new RunJobTask(key, null);
    key = KeyFactory.createKey(JobRecord.DATA_STORE_KIND, "job2");
    RunJobTask runJobTask2 = new RunJobTask(key, null);
    key = KeyFactory.createKey(JobRecord.DATA_STORE_KIND, "job3");
    FinalizeJobTask finalizeJobTask = new FinalizeJobTask(key, null);
    key = KeyFactory.createKey(Slot.DATA_STORE_KIND, "slot1");
    HandleSlotFilledTask hsfTask = new HandleSlotFilledTask(key, null);
    listOfTasks = Lists.<Task> newLinkedList(runJobTask, runJobTask2, finalizeJobTask, hsfTask);
    encodedBytes = FanoutTask.encodeTasks(listOfTasks);
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }


  /**
   * Tests the methods {@link FanoutTask#encodeTasks(java.util.Collection)} and
   * {@link FanoutTask#decodeTasks(byte[])}
   */
  public void testEncodeDecode() throws Exception {
    checkBytes(encodedBytes);
  }

  /**
   * Tests conversion of {@link FanoutTaskRecord} to and from an {@link Entity}
   */
  public void testFanoutTaskRecord() throws Exception {
    Key rootJobKey = KeyFactory.createKey("dummy", "dummy");
    FanoutTaskRecord record = new FanoutTaskRecord(rootJobKey, encodedBytes);
    Entity entity = record.toEntity();
    // reconstitue entity
    record = new FanoutTaskRecord(entity);
    checkBytes(record.getPayload());
  }

  private void checkBytes(byte[] bytes) {
    String encodedString = new String(bytes);
    System.out.println(encodedString);
    List<Task> reconstituted = FanoutTask.decodeTasks(encodedBytes);
    assertEquals(listOfTasks.size(), reconstituted.size());
    for (int i = 0; i < listOfTasks.size(); i++) {
      Task expected = listOfTasks.get(i);
      Task actual = reconstituted.get(i);
      assertEquals(i, expected, actual);
    }
  }

  private void assertEquals(int i, Task expected, Task actual) {
    assertEquals("i=" + i, expected.getType(), actual.getType());
    assertEquals("i=" + i, expected.toProperties(), actual.toProperties());
  }

}
