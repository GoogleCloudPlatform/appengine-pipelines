#!/usr/bin/python2.5
#
# Copyright 2010 Google Inc.
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

"""Tests for common Pipelines."""

import logging
import sys
import unittest

# Fix up paths for running tests.
sys.path.insert(0, '../src/')


from pipeline import testutil
from pipeline import common
from pipeline import pipeline
import test_shared


class CommonTest(test_shared.TaskRunningMixin, unittest.TestCase):

  def setUp(self):
    testutil.setup_for_testing()
    super(CommonTest, self).setUp()

  def testReturn(self):
    self.assertEquals(
        1234, self.run_pipeline(common.Return(1234)).default.value)
    self.assertEquals(
        'hi there',
        self.run_pipeline(common.Return('hi there')).default.value)
    self.assertTrue(self.run_pipeline(common.Return()).default.value is None)

  def testIgnore(self):
    self.assertTrue(
        self.run_pipeline(common.Ignore(1, 2, 3, 4)).default.value is None)

  def testDict(self):
    self.assertEquals(
      {
        'one': 'red',
        'two': 12345,
        'three': [5, 'hello', 6.7],
      },
      self.run_pipeline(
          common.Dict(one='red',
                      two=12345,
                      three=[5, 'hello', 6.7])).default.value)

  def testList(self):
    self.assertEquals(
        [5, 'hello', 6.7],
        self.run_pipeline(common.List(5, 'hello', 6.7)).default.value)

  def testAbortIfTrue_Abort(self):
    try:
      self.run_pipeline(common.AbortIfTrue(True, message='Forced an abort'))
      self.fail('Should have raised')
    except pipeline.Abort, e:
      self.assertEquals('Forced an abort', str(e))

  def testAbortIfTrue_DoNotAbort(self):
    self.run_pipeline(common.AbortIfTrue(False, message='Should not abort'))

  def testAll(self):
    self.assertFalse(self.run_pipeline(common.All()).default.value)
    self.assertFalse(self.run_pipeline(common.All(False, False)).default.value)
    self.assertFalse(self.run_pipeline(common.All(False, True)).default.value)
    self.assertTrue(self.run_pipeline(common.All(True, True)).default.value)

  def testAny(self):
    self.assertFalse(self.run_pipeline(common.Any()).default.value)
    self.assertFalse(self.run_pipeline(common.Any(False, False)).default.value)
    self.assertTrue(self.run_pipeline(common.Any(False, True)).default.value)
    self.assertTrue(self.run_pipeline(common.Any(True, True)).default.value)

  def testComplement(self):
    self.assertEquals(True, self.run_pipeline(
        common.Complement(False)).default.value)
    self.assertEquals(False, self.run_pipeline(
        common.Complement(True)).default.value)
    self.assertEquals([False, True], self.run_pipeline(
        common.Complement(True, False)).default.value)

  def testMax(self):
    self.assertEquals(10, self.run_pipeline(common.Max(1, 10, 5)).default.value)
    self.assertEquals(22, self.run_pipeline(common.Max(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Max)

  def testMin(self):
    self.assertEquals(1, self.run_pipeline(common.Min(1, 10, 5)).default.value)
    self.assertEquals(22, self.run_pipeline(common.Min(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Min)

  def testSum(self):
    self.assertEquals(16, self.run_pipeline(common.Sum(1, 10, 5)).default.value)
    self.assertEquals(22, self.run_pipeline(common.Sum(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Sum)

  def testMultiply(self):
    self.assertEquals(50, self.run_pipeline(
        common.Multiply(1, 10, 5)).default.value)
    self.assertEquals(22, self.run_pipeline(
        common.Multiply(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Multiply)

  def testNegate(self):
    self.assertEquals(-20, self.run_pipeline(
        common.Negate(20)).default.value)
    self.assertEquals(20, self.run_pipeline(
        common.Negate(-20)).default.value)
    self.assertEquals([-20, 15, -2], self.run_pipeline(
        common.Negate(20, -15, 2)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Negate)

  def testExtend(self):
    self.assertEquals([1, 2, 3, 4, 5, 6, 7, 8], self.run_pipeline(
        common.Extend([1, 2, 3], (4, 5, 6), [7], (8,))).default.value)
    self.assertEquals([], self.run_pipeline(
        common.Extend([], (), [], ())).default.value)
    self.assertEquals([1, 2, 3, 4, 5, 6, 7, 8], self.run_pipeline(
        common.Extend([1, 2, 3], [], (4, 5, 6), (), [7], (8,))).default.value)
    self.assertEquals([[1, 2, 3], [4, 5, 6], [7], [8]], self.run_pipeline(
        common.Extend([[1, 2, 3], [4, 5, 6], [7], [8]])).default.value)
    self.assertEquals([], self.run_pipeline(common.Extend()).default.value)

  def testAppend(self):
    self.assertEquals([[1, 2, 3], [4, 5, 6], [7], [8]], self.run_pipeline(
        common.Append([1, 2, 3], [4, 5, 6], [7], [8])).default.value)
    self.assertEquals([[], [], [], []], self.run_pipeline(
        common.Append([], [], [], [])).default.value)
    self.assertEquals([1, 2, 3, 4, 5, 6, 7, 8], self.run_pipeline(
        common.Append(1, 2, 3, 4, 5, 6, 7, 8)).default.value)
    self.assertEquals([], self.run_pipeline(common.Append()).default.value)

  def testConcat(self):
    self.assertEquals('somestringshere', self.run_pipeline(
        common.Concat('some', 'strings', 'here')).default.value)
    self.assertEquals('some|strings|here', self.run_pipeline(
        common.Concat('some', 'strings', 'here', separator='|')).default.value)
    self.assertEquals('', self.run_pipeline(common.Concat()).default.value)

  def testUnion(self):
    self.assertEquals(list(set([1, 2, 3, 4])), self.run_pipeline(
        common.Union([1], [], [2, 3], [], [4])).default.value)
    self.assertEquals([], self.run_pipeline(
        common.Union([], [], [], [])).default.value)
    self.assertEquals([], self.run_pipeline(common.Union()).default.value)

  def testIntersection(self):
    self.assertEquals(list(set([1, 3])), self.run_pipeline(
        common.Intersection([1, 2, 3], [1, 3, 7], [0, 3, 1])).default.value)
    self.assertEquals([], self.run_pipeline(
        common.Intersection([1, 2, 3], [4, 5, 6], [7, 8, 9])).default.value)
    self.assertEquals([], self.run_pipeline(
        common.Intersection([], [], [])).default.value)
    self.assertEquals(
        [], self.run_pipeline(common.Intersection()).default.value)

  def testUniquify(self):
    self.assertEquals(set([3, 2, 1]), set(self.run_pipeline(
        common.Uniquify(1, 2, 3, 3, 2, 1)).default.value))
    self.assertEquals([], self.run_pipeline(common.Uniquify()).default.value)

  def testFormat(self):
    self.assertEquals('this red 14 message', self.run_pipeline(
        common.Format.tuple('this %s %d message', 'red', 14)).default.value)
    self.assertEquals('this red 14 message', self.run_pipeline(
        common.Format.dict(
            'this %(mystring)s %(mynumber)d message',
            mystring='red', mynumber=14)
        ).default.value)
    self.assertEquals('a string here', self.run_pipeline(
        common.Format.tuple('a string here')).default.value)
    self.assertRaises(pipeline.Abort, self.run_pipeline,
        common.Format('blah', 'silly message'))

  def testLog(self):
    saved = []
    def SaveArgs(*args, **kwargs):
      saved.append((args, kwargs))

    self.assertEquals(None, self.run_pipeline(
        common.Log.log(logging.INFO, 'log then %s %d', 'hi', 44)).default.value)

    old_log = common.Log._log_method
    common.Log._log_method = SaveArgs
    try:
      self.run_pipeline(common.Log.log(-333, 'log then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.debug('debug then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.info('info then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.warning('warning then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.error('error then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.critical('critical then %s %d', 'hi', 44))
    finally:
      common.Log._log_method = old_log

    self.assertEquals(saved,
        [
            ((-333, 'log then %s %d', 'hi', 44), {}),
            ((10, 'debug then %s %d', 'hi', 44), {}),
            ((20, 'info then %s %d', 'hi', 44), {}),
            ((30, 'warning then %s %d', 'hi', 44), {}),
            ((40, 'error then %s %d', 'hi', 44), {}),
            ((50, 'critical then %s %d', 'hi', 44), {})
        ])

  def testDelay(self):
    self.assertRaises(TypeError, common.Delay, 1234)
    self.assertRaises(TypeError, common.Delay, stuff=1234)
    self.assertEquals(5, self.run_pipeline(
        common.Delay(seconds=5)).default.value)

  def testEmailToContinue(self):
    old_email_message = common.EmailToContinue._email_message

    saved = []
    def FakeEmailMessage(*args, **kwargs):
      message = old_email_message(*args, **kwargs)
      saved.append(message)
      return message

    mail_kwargs = dict(
      sender='foo@example.com',
      to='meep@example.com',
      subject='hi there',
      body='%(approve_url)s\n%(disapprove_url)s',
      html='<a href="%(approve_url)s">approve</a>\n'
           '<a href="%(disapprove_url)s">disapprove</a>')

    common.EmailToContinue._email_message = FakeEmailMessage
    try:
      stage1 = common.EmailToContinue(**mail_kwargs)
      outputs1 = self.run_pipeline(
          stage1, base_path='/_ah/pipeline', _require_slots_filled=False)

      stage2 = common.EmailToContinue(**mail_kwargs)
      outputs2 = self.run_pipeline(
          stage2, base_path='/_ah/pipeline', _require_slots_filled=False)

      mail_kwargs = mail_kwargs.copy()
      mail_kwargs['approve_html'] = '<h3>Woot approved!</h3>'
      mail_kwargs['disapprove_html'] = '<h3>Doh not approved!</h3>'

      stage3 = common.EmailToContinue(**mail_kwargs)
      outputs3 = self.run_pipeline(
          stage3, base_path='/_ah/pipeline', _require_slots_filled=False)

      stage4 = common.EmailToContinue(**mail_kwargs)
      outputs4 = self.run_pipeline(
          stage4, base_path='/_ah/pipeline', _require_slots_filled=False)

      mail_kwargs = mail_kwargs.copy()
      mail_kwargs['random_token'] = 'banana'

      stage5 = common.EmailToContinue(**mail_kwargs)
      outputs5 = self.run_pipeline(
          stage5,
          idempotence_key='knownid',
          base_path='/_ah/pipeline',
          _require_slots_filled=False)
    finally:
      common.EmailToContinue._email_message = old_email_message

    self.assertEquals(5, len(saved))

    # Uses default approved template.
    result1 = stage1.callback(random_token=stage1.kwargs['random_token'],
                              choice='approve')
    self.assertEquals((200, 'text/html', '<h1>Approved!</h1>'), result1)

    # Uses default disapprove template.
    result2 = stage2.callback(random_token=stage2.kwargs['random_token'],
                              choice='disapprove')
    self.assertEquals((200, 'text/html', '<h1>Not Approved!</h1>'), result2)

    # Uses user-supplied approve template.
    result3 = stage3.callback(random_token=stage3.kwargs['random_token'],
                              choice='approve')
    self.assertEquals((200, 'text/html', '<h3>Woot approved!</h3>'), result3)

    # Uses user-supplied disapprove template.
    result4 = stage4.callback(random_token=stage4.kwargs['random_token'],
                              choice='disapprove')
    self.assertEquals((200, 'text/html', '<h3>Doh not approved!</h3>'), result4)

    # Invalid choice.
    result5 = stage5.callback(random_token=stage4.kwargs['random_token'],
                              choice='invalid')
    self.assertEquals(
        (403, 'text/html', '<h1>Invalid security token.</h1>'), result5)

    # Make sure the string/html templating works.
    message5 = saved[4]

    self.assertEquals(
        '/_ah/pipeline/callback?pipeline_id=knownid'
            '&random_token=banana&choice=approve\n'
        '/_ah/pipeline/callback?pipeline_id=knownid'
            '&random_token=banana&choice=disapprove',
        message5.body)

    self.assertEquals(
        '<a href="/_ah/pipeline/callback?pipeline_id=knownid&amp;'
            'random_token=banana&amp;choice=approve">approve</a>\n'
        '<a href="/_ah/pipeline/callback?pipeline_id=knownid&amp;'
            'random_token=banana&amp;choice=disapprove">disapprove</a>',
        message5.html)

    if not self.test_mode:
      self.assertTrue(common.EmailToContinue.from_id(
                          stage1.pipeline_id).outputs.default.value)
      self.assertFalse(common.EmailToContinue.from_id(
                          stage2.pipeline_id).outputs.default.value)
      self.assertTrue(common.EmailToContinue.from_id(
                          stage3.pipeline_id).outputs.default.value)
      self.assertFalse(common.EmailToContinue.from_id(
                          stage4.pipeline_id).outputs.default.value)
      self.assertFalse(common.EmailToContinue.from_id(
                          stage5.pipeline_id).outputs.default.filled)
    else:
      self.assertTrue(outputs1.default.value)
      self.assertTrue(outputs2.default.value)
      self.assertTrue(outputs3.default.value)
      self.assertTrue(outputs4.default.value)
      self.assertTrue(outputs5.default.value)


class CommonTestModeTest(test_shared.TestModeMixin, CommonTest):
  """Runs all the common library tests in test mode.

  To ensure they can be reused by users in their own functional tests.
  """

  DO_NOT_DELETE = "Seriously... We only need the class declaration."


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
