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


TEST_APP_ID = 'my-app-id'
TEST_VERSION_ID = 'my-version.1234'

# Assign the application ID up front here so we can create db.Key instances
# before doing any other test setup.
os.environ['APPLICATION_ID'] = TEST_APP_ID
os.environ['CURRENT_VERSION_ID'] = TEST_VERSION_ID
os.environ['HTTP_HOST'] = '%s.appspot.com' % TEST_APP_ID
os.environ['DEFAULT_VERSION_HOSTNAME'] = os.environ['HTTP_HOST']
os.environ['CURRENT_MODULE_ID'] = 'foo-module'


def setup_for_testing(require_indexes=True, define_queues=[]):
  """Sets up the stubs for testing.

  Args:
    require_indexes: True if indexes should be required for all indexes.
    define_queues: Additional queues that should be available.
  """
  from google.appengine.api import apiproxy_stub_map
  from google.appengine.api import memcache
  from google.appengine.api import queueinfo
  from google.appengine.datastore import datastore_stub_util
  from google.appengine.tools import old_dev_appserver
  from google.appengine.tools import dev_appserver_index
  before_level = logging.getLogger().getEffectiveLevel()
  try:
    logging.getLogger().setLevel(100)
    root_path = os.path.realpath(os.path.dirname(__file__))
    old_dev_appserver.SetupStubs(
        TEST_APP_ID,
        root_path=root_path,
        login_url='',
        datastore_path=tempfile.mktemp(suffix='datastore_stub'),
        history_path=tempfile.mktemp(suffix='datastore_history'),
        blobstore_path=tempfile.mktemp(suffix='blobstore_stub'),
        require_indexes=require_indexes,
        clear_datastore=False)
    dev_appserver_index.SetupIndexes(TEST_APP_ID, root_path)
    # Actually need to flush, even though we've reallocated. Maybe because the
    # memcache stub's cache is at the module level, not the API stub?
    memcache.flush_all()
  finally:
    logging.getLogger().setLevel(before_level)

  datastore_stub = apiproxy_stub_map.apiproxy.GetStub('datastore_v3')
  hr_policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
  datastore_stub.SetConsistencyPolicy(hr_policy)

  taskqueue_stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
  taskqueue_stub.queue_yaml_parser = (
      lambda x: queueinfo.LoadSingleQueue(
          'queue:\n- name: default\n  rate: 1/s\n' +
          '\n'.join('- name: %s\n  rate: 1/s' % name
                    for name in define_queues)))
