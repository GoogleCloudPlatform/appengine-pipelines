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

import static com.google.appengine.tools.pipeline.impl.util.GUIDGenerator.USE_SIMPLE_GUIDS_FOR_DEBUGGING;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.repackaged.com.google.common.collect.ImmutableList;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.Slot;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class BarrierTest extends TestCase {

  private LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testArgumentBuilding() throws Exception {
    doArgumentBuildingTest(new Integer[] {});
    doArgumentBuildingTest(new String[] {"hello"}, "hello");
    doArgumentBuildingTest(new Integer[] {5, 7}, 5, 7);
    doArgumentBuildingTest(new Object[] {"hello", 5, null}, "hello", 5, null);
    doArgumentBuildingTest(new Object[] {6, 8}, new PhantomMarker(5), 6, new PhantomMarker(7), 8);
    doArgumentBuildingTest(new Object[] {Lists.newArrayList(1, 2, 3)}, new ListMarker(1, 2, 3));
    doArgumentBuildingTest(new Object[] {Lists.newArrayList(1, 2, 3)}, Lists
        .newArrayList(1, 2, 3));
    doArgumentBuildingTest(new Object[] {Lists.newArrayList(1, 2, 3),
        Lists.newArrayList("red", "blue")}, Lists.newArrayList(1, 2, 3), Lists.newArrayList(
        "red", "blue"));
    doArgumentBuildingTest(new Object[] {"hello", 5, Lists.newArrayList(1, 2, 3), "apple",
        Lists.newArrayList(2, 3, 4), Lists.newArrayList(4, 5, 6), Lists.newArrayList(7),
        Lists.newArrayList("red", "blue")}, "hello", 5, new PhantomMarker("goodbye"),
        new ListMarker(1, 2, 3), "apple", new ListMarker(2, 3, 4), new ListMarker(4, 5, 6),
        new PhantomMarker("banana"), new ListMarker(7), Lists.newArrayList("red", "blue"));
  }

  private static class ListMarker {
    public List<?> valueList;

    ListMarker(Object... elements) {
      valueList = ImmutableList.copyOf(elements);
    }
  }

  private static class PhantomMarker {
    Object value;

    PhantomMarker(Object v) {
      value = v;
    }
  }

  private void doArgumentBuildingTest(Object[] expectedArguments, Object... slotValues) {
    Barrier barrier = Barrier.dummyInstanceForTesting();
    for (Object value : slotValues) {
      if (value instanceof ListMarker) {
        List<?> valueList = ((ListMarker) value).valueList;
        List<Slot> slotList = new ArrayList<>(valueList.size());
        Slot dummyListSlot = createDummySlot();
        dummyListSlot.fill(null);
        for (Object v : valueList) {
          Slot slot = createDummySlot();
          slot.fill(v);
          slotList.add(slot);
        }
        barrier.addListArgumentSlots(dummyListSlot, slotList);
      } else if (value instanceof PhantomMarker) {
        Slot slot = createDummySlot();
        slot.fill(((PhantomMarker) value).value);
        barrier.addPhantomArgumentSlot(slot);
      } else {
        Slot slot = createDummySlot();
        slot.fill(value);
        barrier.addRegularArgumentSlot(slot);
      }

    }
    Object[] arguments = barrier.buildArgumentArray();
    assertEqualArrays(expectedArguments, arguments);
  }

  private void assertEqualArrays(Object[] expected, Object[] actual) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals("i=" + i, expected[i], actual[i]);
    }
  }

  public static Slot createDummySlot() {
    Key dummyKey = KeyFactory.createKey("dummy", "dummy");
    return new Slot(dummyKey, dummyKey, "abc");
  }
}
