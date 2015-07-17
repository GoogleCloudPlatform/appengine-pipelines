#!/usr/bin/env python
#
# Copyright 2009 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Test utilities for the Google App Engine Pipeline API."""

# Code originally from:
#   http://code.google.com/p/pubsubhubbub/source/browse/trunk/hub/testutil.py

import logging
import os
import sys
import tempfile

class TestSetupMixin(object):

  TEST_APP_ID = 'my-app-id'
  TEST_VERSION_ID = 'my-version.1234'

  def setUp(self):
    super(TestSetupMixin, self).setUp()

    from google.appengine.api import apiproxy_stub_map
    from google.appengine.api import memcache
    from google.appengine.api import queueinfo
    from google.appengine.datastore import datastore_stub_util
    from google.appengine.ext import testbed
    from google.appengine.ext.testbed import TASKQUEUE_SERVICE_NAME

    before_level = logging.getLogger().getEffectiveLevel()

    os.environ['APPLICATION_ID'] = self.TEST_APP_ID
    os.environ['CURRENT_VERSION_ID'] = self.TEST_VERSION_ID
    os.environ['HTTP_HOST'] = '%s.appspot.com' % self.TEST_APP_ID
    os.environ['DEFAULT_VERSION_HOSTNAME'] = os.environ['HTTP_HOST']
    os.environ['CURRENT_MODULE_ID'] = 'foo-module'
    
    try:
      logging.getLogger().setLevel(100)

      self.testbed = testbed.Testbed()
      self.testbed.activate()
      self.testbed.setup_env(app_id=self.TEST_APP_ID, overwrite=True)
      self.testbed.init_memcache_stub()

      hr_policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
      self.testbed.init_datastore_v3_stub(consistency_policy=hr_policy)

      self.testbed.init_taskqueue_stub()

      root_path = os.path.realpath(os.path.dirname(__file__))
      
      # Actually need to flush, even though we've reallocated. Maybe because the
      # memcache stub's cache is at the module level, not the API stub?
      memcache.flush_all()
    finally:
      logging.getLogger().setLevel(before_level)

    define_queues=['other']
    taskqueue_stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
    taskqueue_stub.queue_yaml_parser = (
        lambda x: queueinfo.LoadSingleQueue(
            'queue:\n- name: default\n  rate: 1/s\n' +
            '\n'.join('- name: %s\n  rate: 1/s' % name
                      for name in define_queues)))

  def tearDown(self):
    super(TestSetupMixin, self).tearDown()
    self.testbed.deactivate()
