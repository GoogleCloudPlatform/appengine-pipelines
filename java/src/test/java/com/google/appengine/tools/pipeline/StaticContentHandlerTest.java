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

import com.google.appengine.tools.pipeline.impl.servlets.StaticContentHandler;

import junit.framework.TestCase;

import java.io.InputStream;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class StaticContentHandlerTest extends TestCase {

  public void testGetResourceAsStream() throws Exception {
    InputStream in = StaticContentHandler.getResourceAsStream("common.js");
    assertTrue(in.read() != -1);
    in = StaticContentHandler.getResourceAsStream("images/treeview-black.gif");
    assertTrue(in.read() != -1);
  }

}
