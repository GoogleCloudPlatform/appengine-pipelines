#!/usr/bin/env python
"""Tests for util.py."""

import datetime
import logging
import os
import sys
import unittest

# Fix up paths for running tests.
sys.path.insert(0, "../src/")

from pipeline import util

from google.appengine.api import taskqueue


class JsonSerializationTest(unittest.TestCase):
  """Test custom json encoder and decoder."""

  def testE2e(self):
    now = datetime.datetime.now()
    obj = {"a": 1, "b": [{"c": "d"}], "e": now}
    new_obj = util.json.loads(util.json.dumps(
        obj, cls=util.JsonEncoder), cls=util.JsonDecoder)
    self.assertEquals(obj, new_obj)


class GetTaskTargetTest(unittest.TestCase):

  def setUp(self):
    super(GetTaskTargetTest, self).setUp()
    os.environ["CURRENT_VERSION_ID"] = "v7.1"
    os.environ["CURRENT_MODULE_ID"] = "foo-module"

  def testGetTaskTarget(self):
    self.assertEqual("v7.foo-module", util._get_task_target())
    task = taskqueue.Task(url="/relative_url",
                          target=util._get_task_target())
    self.assertEqual("v7.foo-module", task.target)

  def testGetTaskTargetDefaultModule(self):
    os.environ["CURRENT_MODULE_ID"] = "default"
    self.assertEqual("v7.default", util._get_task_target())
    task = taskqueue.Task(url="/relative_url",
                          target=util._get_task_target())
    self.assertEqual("v7.default", task.target)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
