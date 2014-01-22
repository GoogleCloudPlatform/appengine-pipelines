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

package com.google.appengine.tools.pipeline.impl.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class JsonUtils {

  public static String mapToJson(Map<?, ?> map) {
    try {
      return (new JSONObject(map)).toString(2);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // All code below is only for manually testing this class.

  /**
   * Convert an object into its JSON representation.
   */
  private static String toJson(Object x) {
    try {
      if (x == null || x instanceof String || x instanceof Number || x instanceof Character
          || x.getClass().isArray() || x instanceof Iterable<?>) {
        return new JSONObject().put("JSON", x).toString(2);
      } else if (x instanceof Map<?, ?>) {
        return (new JSONObject((Map<?, ?>) x)).toString(2);
      } else if (x instanceof JSONObject) {
        return ((JSONObject) x).toString(2);
      } else {
        return (new JSONObject(x)).toString(2);
      }
    } catch (Exception e) {
      throw new RuntimeException("x=" + x, e);
    }
  }

  /**
   * Convert a JSON representation into an object
   */
  public static Object fromJson(String json) {
    try {
      JSONObject jsonObject = new JSONObject(json);
      if (jsonObject.has("JSON")) {
        return convert(jsonObject.get("JSON"));
      } else {
        return convert(jsonObject);
      }
    } catch (Exception e) {
      throw new RuntimeException("json=" + json, e);
    }
  }

  /**
   * Convert an <code>org.json.JSONObject</code> into a <code>Map</code> and an
   * <code>org.json.JSONArray</code> into a <code>List</code>
   */
  private static Object convert(Object x) throws JSONException {
    if (x instanceof JSONObject) {
      JSONObject jsonObject = (JSONObject) x;
      String[] names = JSONObject.getNames(jsonObject);
      if (names == null || names.length == 0) {
        return new HashMap<>(0);
      }
      HashMap<String, Object> map = new HashMap<>(names.length);
      for (String name : names) {
        Object value = jsonObject.get(name);
        map.put(name, convert(value));
      }
      return map;
    } else if (x instanceof JSONArray) {
      JSONArray jsonArray = (JSONArray) x;
      int length = jsonArray.length();
      List<Object> list = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        list.add(convert(jsonArray.get(i)));
      }
      return list;
    } else {
      return x;
    }
  }

  private static String recursiveToString(Object y) {
    StringBuilder builder = new StringBuilder(512);
    if (null == y) {
      builder.append("null");
    } else {
      if (y instanceof List) {
        List<?> list = (List<?>) y;
        builder.append("(");
        boolean first = true;
        for (Object x : list) {
          if (!first) {
            builder.append(", ");
          }
          builder.append(recursiveToString(x));
          first = false;
        }
        builder.append(")");
      } else if (y instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) y;
        builder.append("{");
        boolean first = true;
        for (Object key : map.keySet()) {
          if (!first) {
            builder.append(", ");
          }
          builder.append(key);
          builder.append("=");
          builder.append(recursiveToString(map.get(key)));
          first = false;
        }
        builder.append("}");
      } else if (y instanceof String) {
        builder.append('"');
        builder.append(y);
        builder.append('"');
      } else {
        builder.append(y);
      }
    }
    return builder.toString();
  }

  private static void debugPrint(Object x) {
    System.out.println();
    String json = toJson(x);
    Object y = fromJson(json);
    System.out.println(x + " --> " + json + " --> " + recursiveToString(y));
  }

  public static void main(String[] args) throws Exception {
    JSONObject x = new JSONObject();
    x.put("first", 5);
    x.put("second", 7);
    debugPrint("hello");
    debugPrint(7);
    debugPrint(3.14159);
    debugPrint("");
    debugPrint('x');
    debugPrint(x);
    debugPrint(null);

    Map<String, Integer> map = new HashMap<>();
    map.put("first", 5);
    map.put("second", 7);
    debugPrint(map);

    int[] array = new int[] {5, 7};
    debugPrint(array);

    ArrayList<Integer> arrayList = new ArrayList<>(2);
    arrayList.add(5);
    arrayList.add(7);
    debugPrint(arrayList);

    Collection<Integer> collection = new HashSet<>(2);
    collection.add(5);
    collection.add(7);
    debugPrint(collection);

    Object object = new Object();
    debugPrint(object);

    Map<String, String> map1 = new HashMap<>();
    map1.put("a", "hello");
    map1.put("b", "goodbye");

    Object[] array2 = new Object[] {17, "yes", "no", map1};

    Map<String, Object> map2 = new HashMap<>();
    map2.put("first", 5.4);
    map2.put("second", array2);
    map2.put("third", map1);

    debugPrint(map2);

    class MyBean {
      @SuppressWarnings("unused")
      public int getX() {
        return 11;
      }

      @SuppressWarnings("unused")
      public boolean isHot() {
        return true;
      }

      @SuppressWarnings("unused")
      public String getName() {
        return "yellow";
      }
    }
    debugPrint(new MyBean());

  }
}
