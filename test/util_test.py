#!/usr/bin/python2.5
"""Tests for util.py."""

import datetime
import sys
import unittest

# Fix up paths for running tests.
sys.path.insert(0, "../src/")

from pipeline import util


class JsonSerializationTest(unittest.TestCase):
  """Test custom json encoder and decoder."""

  def testE2e(self):
    now = datetime.datetime.now()
    obj = {"a": 1, "b": [{"c": "d"}], "e": now}
    new_obj = util.simplejson.loads(util.simplejson.dumps(
        obj, cls=util.JsonEncoder), cls=util.JsonDecoder)
    self.assertEquals(obj, new_obj)
