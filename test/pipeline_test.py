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

"""Tests for the Pipeline API."""

from __future__ import with_statement

import base64
import datetime
import logging
import os
import pickle
import sys
import unittest
import urllib

# Fix up paths for running tests.
sys.path.insert(0, '../src/')

from pipeline import simplejson

from pipeline import testutil
from pipeline import common
from pipeline import models
from pipeline import pipeline
import test_shared

from google.appengine.api import mail
from google.appengine.ext import blobstore
from google.appengine.ext import db

# For convenience.
_PipelineRecord = pipeline.models._PipelineRecord
_SlotRecord = pipeline.models._SlotRecord
_BarrierRecord = pipeline.models._BarrierRecord
_StatusRecord = pipeline.models._StatusRecord


class TestBase(unittest.TestCase):
  """Base class for all tests in this module."""

  def setUp(self):
    testutil.setup_for_testing(define_queues=['other'])
    super(TestBase, self).setUp()


class SlotTest(TestBase):
  """Tests for the Slot class."""

  def testCreate(self):
    """Tests creating Slots with names and keys."""
    slot = pipeline.Slot(name='stuff')
    self.assertEquals('stuff', slot.name)
    self.assertTrue(slot.key)
    self.assertFalse(slot.filled)
    self.assertFalse(slot._exists)
    self.assertRaises(pipeline.SlotNotFilledError, lambda: slot.value)
    self.assertRaises(pipeline.SlotNotFilledError, lambda: slot.filler)
    self.assertRaises(pipeline.SlotNotFilledError, lambda: slot.fill_datetime)

    slot_key = db.Key.from_path('mykind', 'mykey')
    slot = pipeline.Slot(name='stuff', slot_key=slot_key)
    self.assertEquals('stuff', slot.name)
    self.assertEquals(slot_key, slot.key)
    self.assertFalse(slot.filled)
    self.assertTrue(slot._exists)

    self.assertRaises(pipeline.UnexpectedPipelineError, pipeline.Slot)

  def testSlotRecord(self):
    """Tests filling Slot attributes with a _SlotRecord."""
    slot_key = db.Key.from_path('myslot', 'mykey')
    filler_key = db.Key.from_path('myfiller', 'mykey')
    now = datetime.datetime.utcnow()
    slot_record = _SlotRecord(
        filler=filler_key,
        value=simplejson.dumps('my value'),
        status=_SlotRecord.FILLED,
        fill_time=now)

    slot = pipeline.Slot(name='stuff', slot_key=slot_key)
    slot._set_value(slot_record)
    self.assertTrue(slot._exists)
    self.assertTrue(slot.filled)
    self.assertEquals('my value', slot.value)
    self.assertEquals(filler_key.name(), slot.filler)
    self.assertEquals(now, slot.fill_datetime)

  def testValueTestMode(self):
    """Tests filling Slot attributes for test mode."""
    slot_key = db.Key.from_path('myslot', 'mykey')
    filler_key = db.Key.from_path('myfiller', 'mykey')
    now = datetime.datetime.utcnow()
    value = 'my value'

    slot = pipeline.Slot(name='stuff', slot_key=slot_key)
    slot._set_value_test(filler_key, value)
    self.assertTrue(slot._exists)
    self.assertTrue(slot.filled)
    self.assertEquals('my value', slot.value)
    self.assertEquals(filler_key.name(), slot.filler)
    self.assertTrue(isinstance(slot.fill_datetime, datetime.datetime))


class PipelineFutureTest(TestBase):
  """Tests for the PipelineFuture class."""

  def testNormal(self):
    """Tests using a PipelineFuture in normal mode."""
    future = pipeline.PipelineFuture([])
    self.assertTrue('default' in future._output_dict)
    default = future.default
    self.assertTrue(isinstance(default, pipeline.Slot))
    self.assertFalse(default.filled)

    self.assertFalse('stuff' in future._output_dict)
    stuff = future.stuff
    self.assertTrue('stuff' in future._output_dict)
    self.assertNotEquals(stuff.key, default.key)
    self.assertTrue(isinstance(stuff, pipeline.Slot))
    self.assertFalse(stuff.filled)

  def testStrictMode(self):
    """Tests using a PipelineFuture that's in strict mode."""
    future = pipeline.PipelineFuture(['one', 'two'])
    self.assertTrue(future._strict)
    self.assertTrue('default' in future._output_dict)
    self.assertTrue('one' in future._output_dict)
    self.assertTrue('two' in future._output_dict)

    default = future.default
    self.assertTrue(isinstance(default, pipeline.Slot))
    self.assertFalse(default.filled)

    one = future.one
    self.assertTrue(isinstance(one, pipeline.Slot))
    self.assertFalse(one.filled)
    self.assertNotEquals(one.key, default.key)

    two = future.two
    self.assertTrue(isinstance(two, pipeline.Slot))
    self.assertFalse(two.filled)
    self.assertNotEquals(two.key, default.key)
    self.assertNotEquals(two.key, one.key)

    self.assertRaises(pipeline.SlotNotDeclaredError, lambda: future.three)

  def testReservedOutputs(self):
    """Tests reserved output slot names."""
    self.assertRaises(pipeline.UnexpectedPipelineError,
                      pipeline.PipelineFuture, ['default'])

  def testInheritOutputs(self):
    """Tests _inherit_outputs without resolving their values."""
    future = pipeline.PipelineFuture([])
    already_defined = {
        'one': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist1')),
        'two': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist2')),
        'three': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist3')),
        'default': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist4')),
    }
    future = pipeline.PipelineFuture([])
    self.assertFalse(future.default._exists)

    future._inherit_outputs('mypipeline', already_defined)
    
    self.assertEquals(already_defined['one'], str(future.one.key))
    self.assertEquals(already_defined['two'], str(future.two.key))
    self.assertEquals(already_defined['three'], str(future.three.key))
    self.assertEquals(already_defined['default'], str(future.default.key))

    self.assertTrue(future.one._exists)
    self.assertTrue(future.two._exists)
    self.assertTrue(future.three._exists)
    self.assertTrue(future.default._exists)

  def testInheritOutputsStrictMode(self):
    """Tests _inherit_outputs without resolving their values in strict mode."""
    already_defined = {
        'one': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist1')),
        'two': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist2')),
        'three': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist3')),
        'default': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist4')),
    }
    future = pipeline.PipelineFuture(['one', 'two', 'three'])

    self.assertFalse(future.one._exists)
    self.assertFalse(future.two._exists)
    self.assertFalse(future.three._exists)
    self.assertFalse(future.default._exists)

    future._inherit_outputs('mypipeline', already_defined)

    self.assertEquals(already_defined['one'], str(future.one.key))
    self.assertEquals(already_defined['two'], str(future.two.key))
    self.assertEquals(already_defined['three'], str(future.three.key))
    self.assertEquals(already_defined['default'], str(future.default.key))

    self.assertTrue(future.one._exists)
    self.assertTrue(future.two._exists)
    self.assertTrue(future.three._exists)
    self.assertTrue(future.default._exists)

  def testInheritOutputsStrictModeUndeclared(self):
    """Tests _inherit_outputs when an inherited output has not been declared."""
    already_defined = {
        'one': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist1')),
        'two': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist2')),
        'three': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist3')),
        'default': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist4')),
        'five': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist5')),
    }
    future = pipeline.PipelineFuture(['one', 'two', 'three'])
    self.assertRaises(pipeline.UnexpectedPipelineError, future._inherit_outputs,
                      'mypipeline', already_defined)

  def testInheritOutputsResolveValues(self):
    """Tests _inherit_outputs with resolving their current values."""
    one = _SlotRecord(
        value=simplejson.dumps('hi one'),
        status=_SlotRecord.FILLED,
        fill_time=datetime.datetime.utcnow(),
        filler=db.Key.from_path('mykind', 'mykey1'))
    one.put()

    two = _SlotRecord(
        value=simplejson.dumps('hi two'),
        status=_SlotRecord.FILLED,
        fill_time=datetime.datetime.utcnow(),
        filler=db.Key.from_path('mykind', 'mykey2'))
    two.put()

    three = _SlotRecord()
    three.put()

    default = _SlotRecord()
    default.put()

    already_defined = {
        'one': str(one.key()),
        'two': str(two.key()),
        'three': str(three.key()),
        'default': str(default.key()),
    }
    future = pipeline.PipelineFuture([])
    future._inherit_outputs('mypipeline', already_defined, resolve_outputs=True)

    self.assertEquals('hi one', future.one.value)
    self.assertEquals('hi two', future.two.value)
    self.assertFalse(future.three.filled)

  def testInheritOutputsResolveValuesMissing(self):
    """Tests when output _SlotRecords are missing for inherited outputs."""
    already_defined = {
        'four': str(db.Key.from_path(_SlotRecord.kind(), 'does not exist')),
    }
    future = pipeline.PipelineFuture([])
    self.assertRaises(pipeline.UnexpectedPipelineError, future._inherit_outputs,
                      'mypipeline', already_defined, resolve_outputs=True)


class NothingPipeline(pipeline.Pipeline):
  """Pipeline that does nothing."""

  output_names = ['one', 'two']

  def run(self):
    self.fill('one', 1)
    self.fill('two', 1)


class OutputlessPipeline(pipeline.Pipeline):
  """Pipeline that outputs nothing."""

  def run(self):
    pass


class AsyncOutputlessPipeline(pipeline.Pipeline):
  """Pipeline that outputs nothing."""

  async = True

  def run(self):
    self.complete()


class AsyncCancellable(pipeline.Pipeline):
  """Pipeline that can be cancelled."""

  async = True

  def run(self):
    self.complete()

  def try_cancel(self):
    return True


class PipelineTest(TestBase):
  """Tests for the Pipeline class."""

  def testClassPath(self):
    """Tests the class path resolution class method."""
    module_dict = {}
    self.assertEquals(None, pipeline.Pipeline._class_path)
    pipeline.Pipeline._set_class_path(module_dict)
    self.assertEquals(None, pipeline.Pipeline._class_path)

    NothingPipeline._class_path = None
    self.assertRaises(ImportError, NothingPipeline._set_class_path,
                      module_dict=module_dict)
    self.assertEquals(None, NothingPipeline._class_path)

    class MyModule(object):
      pass

    mymodule = MyModule()
    setattr(mymodule, 'NothingPipeline', NothingPipeline)

    # Does not require __main__.
    module_dict['other'] = mymodule
    NothingPipeline._set_class_path(module_dict=module_dict)
    self.assertEquals('other.NothingPipeline', NothingPipeline._class_path)

    # Will ignore __main__.
    NothingPipeline._class_path = None
    module_dict['__main__'] = mymodule
    NothingPipeline._set_class_path(module_dict=module_dict)
    self.assertEquals('other.NothingPipeline', NothingPipeline._class_path)

    # Will use __main__ as a last resort.
    NothingPipeline._class_path = None
    del module_dict['other']
    NothingPipeline._set_class_path(module_dict=module_dict)
    self.assertEquals('__main__.NothingPipeline', NothingPipeline._class_path)

    # Will break if could not find class name and it's not in __main__.
    NothingPipeline._class_path = None
    setattr(mymodule, 'NothingPipeline', object())
    module_dict = {'__main__': mymodule}
    self.assertRaises(ImportError, NothingPipeline._set_class_path,
                      module_dict=module_dict)

  def testStart(self):
    """Tests starting a Pipeline."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    self.assertEquals(('one', 'two'), stage.args)
    self.assertEquals({'three': 'red', 'four': 1234}, stage.kwargs)

    self.assertTrue(stage.start() is None)
    self.assertEquals('default', stage.queue_name)
    self.assertEquals('/_ah/pipeline', stage.base_path)
    self.assertEquals(stage.pipeline_id, stage.root_pipeline_id)
    self.assertTrue(stage.is_root)

    pipeline_record = _PipelineRecord.get_by_key_name(stage.pipeline_id)
    self.assertTrue(pipeline_record is not None)
    self.assertEquals('__main__.NothingPipeline', pipeline_record.class_path)
    self.assertEquals(_PipelineRecord.WAITING, pipeline_record.status)

    params = pipeline_record.params
    self.assertEquals(params['args'],
        [{'type': 'value', 'value': 'one'}, {'type': 'value', 'value': 'two'}])
    self.assertEquals(params['kwargs'],
        {'four': {'type': 'value', 'value': 1234},
         'three': {'type': 'value', 'value': 'red'}})
    self.assertEquals([], params['after_all'])
    self.assertEquals('default', params['queue_name'])
    self.assertEquals('/_ah/pipeline', params['base_path'])
    self.assertEquals(set(NothingPipeline.output_names + ['default']),
                      set(params['output_slots'].keys()))
    self.assertTrue(pipeline_record.is_root_pipeline)
    self.assertTrue(isinstance(pipeline_record.start_time, datetime.datetime))

    # Verify that all output slots are present.
    slot_records = list(_SlotRecord.all().filter(
      'root_pipeline =',
      db.Key.from_path(_PipelineRecord.kind(), stage.pipeline_id)))
    slot_dict = dict((s.key(), s) for s in slot_records)
    self.assertEquals(3, len(slot_dict))

    for outputs in params['output_slots'].itervalues():
      slot_record = slot_dict[db.Key(outputs)]
      self.assertEquals(_SlotRecord.WAITING, slot_record.status)

    # Verify that trying to add another output slot will fail.
    self.assertRaises(pipeline.SlotNotDeclaredError,
                      lambda: stage.outputs.does_not_exist)

    # Verify that the slot existence has been set to true.
    for slot in stage.outputs._output_dict.itervalues():
      self.assertTrue(slot._exists)

    # Verify the enqueued task.
    task_list = test_shared.get_tasks()
    self.assertEquals(1, len(task_list))
    task = task_list[0]
    self.assertEquals(
        {'pipeline_key': [str(db.Key.from_path(
            _PipelineRecord.kind(), stage.pipeline_id))]},
        task['params'])
    self.assertEquals('/_ah/pipeline/run', task['url'])

  def testStartIdempotenceKey(self):
    """Tests starting a pipeline with an idempotence key."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    self.assertTrue(stage.start(idempotence_key='banana') is None)
    self.assertEquals('banana', stage.pipeline_id)

  def testStartReturnTask(self):
    """Tests starting a pipeline and returning the kick-off task."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    task = stage.start(return_task=True, idempotence_key='banana')
    self.assertEquals(0, len(test_shared.get_tasks()))
    self.assertEquals('/_ah/pipeline/run', task.url)
    self.assertEquals(
        'pipeline_key=%s' % db.Key.from_path(_PipelineRecord.kind(), 'banana'),
        task.payload)
    self.assertTrue(task.name is None)

  def testStartQueueName(self):
    """Tests that the start queue name will be preserved."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    self.assertTrue(stage.start(queue_name='other') is None)
    self.assertEquals(0, len(test_shared.get_tasks('default')))
    self.assertEquals(1, len(test_shared.get_tasks('other')))

  def testStartUndeclaredOutputs(self):
    """Tests that accessing undeclared outputs on a root pipeline will err.

    Only applies to root pipelines that have no named outputs and only have
    the default output slot.
    """
    stage = OutputlessPipeline()
    stage.start()
    self.assertFalse(stage.outputs.default.filled)
    self.assertRaises(pipeline.SlotNotDeclaredError, lambda: stage.outputs.blah)

  def testStartIdempotenceKeyExists(self):
    """Tests when the idempotence key is a dupe."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    other_stage = OutputlessPipeline()
    self.assertRaises(pipeline.PipelineExistsError,
                      other_stage.start, idempotence_key='banana')

  def testStartRetryParameters(self):
    """Tests setting retry backoff parameters before calling start()."""
    stage = OutputlessPipeline()
    stage.max_attempts = 15
    stage.backoff_seconds = 1234.56
    stage.backoff_factor = 2.718
    stage.start(idempotence_key='banana')
    pipeline_record = _PipelineRecord.get_by_key_name(stage.pipeline_id)
    self.assertTrue(pipeline_record is not None)
    self.assertEquals(15, pipeline_record.params['max_attempts'])
    self.assertEquals(1234.56, pipeline_record.params['backoff_seconds'])
    self.assertEquals(2.718, pipeline_record.params['backoff_factor'])

  def testStartException(self):
    """Tests when a dependent method from start raises an exception."""
    def mock_raise(*args, **kwargs):
      raise Exception('Doh! Fake error')

    stage = OutputlessPipeline()
    stage._set_values_internal = mock_raise
    try:
      stage.start(idempotence_key='banana')
      self.fail('Did not raise')
    except pipeline.PipelineSetupError, e:
      self.assertEquals(
          'Error starting __main__.OutputlessPipeline(*(), **{})#banana: '
          'Doh! Fake error',
          str(e))

  def testFromId(self):
    """Tests retrieving a Pipeline instance by ID."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.max_attempts = 15
    stage.backoff_seconds = 1234.56
    stage.backoff_factor = 2.718
    stage.target = 'my-other-target'
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEquals(('one', 'two'), other.args)
    self.assertEquals({'three': 'red', 'four': 1234}, other.kwargs)
    self.assertEquals('other', other.queue_name)
    self.assertEquals('/other', other.base_path)
    self.assertEquals('meep', other.pipeline_id)
    self.assertEquals('meep', other.root_pipeline_id)
    self.assertTrue(other.is_root)
    self.assertEquals(15, other.max_attempts)
    self.assertEquals(1234.56, other.backoff_seconds)
    self.assertEquals(2.718, other.backoff_factor)
    self.assertEquals('my-other-target', other.target)
    self.assertEquals(1, other.current_attempt)

    self.assertFalse(other.outputs.one.filled)
    self.assertEquals(stage.outputs.one.key, other.outputs.one.key)
    self.assertFalse(other.outputs.two.filled)
    self.assertEquals(stage.outputs.two.key, other.outputs.two.key)

  def testFromIdResolveOutputs(self):
    """Tests retrieving a Pipeline instance by ID and resolving its outputs."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    stage.fill('one', 'red')
    stage.fill('two', 'blue')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertTrue(other.outputs.one.filled)
    self.assertEquals(stage.outputs.one.key, other.outputs.one.key)
    self.assertEquals('red', other.outputs.one.value)
    self.assertTrue(other.outputs.two.filled)
    self.assertEquals(stage.outputs.two.key, other.outputs.two.key)
    self.assertEquals('blue', other.outputs.two.value)

  def testFillString(self):
    """Tests filling a slot by name."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    stage.fill('one', 'red')
    stage.fill('two', 'blue')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEquals('red', other.outputs.one.value)
    self.assertEquals('blue', other.outputs.two.value)

  def testFillSlot(self):
    """Tests filling a slot with a Slot instance."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    stage.fill(stage.outputs.one, 'red')
    stage.fill(stage.outputs.two, 'blue')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEquals('red', other.outputs.one.value)
    self.assertEquals('blue', other.outputs.two.value)

  def testFillSlot_Huge(self):
    """Tests filling a slot with over 1MB of data."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')

    big_data = 'red' * 1000000
    self.assertTrue(len(big_data) > 1000000)
    small_data = 'blue' * 500
    self.assertTrue(len(small_data) < 1000000)

    stage.fill(stage.outputs.one, big_data)
    stage.fill(stage.outputs.two, small_data)

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEquals(big_data, other.outputs.one.value)
    self.assertEquals(small_data, other.outputs.two.value)

  def testFillSlotErrors(self):
    """Tests errors that happen when filling slots."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    self.assertRaises(pipeline.UnexpectedPipelineError,
                      stage.fill, object(), 'red')

    slot = pipeline.Slot(name='one')
    self.assertRaises(pipeline.SlotNotDeclaredError,
                      stage.fill, slot, 'red')

    db.delete(stage.outputs.one.key)
    self.assertRaises(pipeline.UnexpectedPipelineError,
                      stage.fill, stage.outputs.one, 'red')

  def testComplete(self):
    """Tests asynchronous completion of the pipeline."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage.complete(1234)

    other = AsyncOutputlessPipeline.from_id(stage.pipeline_id)
    self.assertEquals(1234, other.outputs.default.value)

  def testCompleteDisallowed(self):
    """Tests completion of the pipeline when it's not asynchronous."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start()
    self.assertRaises(pipeline.UnexpectedPipelineError, stage.complete)

  def testGetCallbackUrl(self):
    """Tests the get_callback_url method."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    result = stage.get_callback_url(one='red', two='blue', three=12345)
    self.assertEquals(
        '/_ah/pipeline/callback'
        '?pipeline_id=banana&three=12345&two=blue&one=red',
        result)

  def testGetCallbackTask(self):
    """Tests the get_callback_task method."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    now = datetime.datetime.utcnow()
    task = stage.get_callback_task(
        params=dict(one='red', two='blue', three=12345),
        method='overridden',
        name='my-name',
        eta=now)
    self.assertEquals('/_ah/pipeline/callback', task.url)
    self.assertEquals(
        'pipeline_id=banana&three=12345&two=blue&one=red', task.payload)
    self.assertEquals('POST', task.method)
    self.assertEquals('my-name', task.name)
    self.assertEquals(now, task.eta.replace(tzinfo=None))

  def testAccesorsUnknown(self):
    """Tests using accessors when they have unknown values."""
    stage = OutputlessPipeline()
    self.assertTrue(stage.pipeline_id is None)
    self.assertTrue(stage.root_pipeline_id is None)
    self.assertTrue(stage.queue_name is None)
    self.assertTrue(stage.base_path is None)
    self.assertFalse(stage.has_finalized)
    self.assertFalse(stage.was_aborted)
    self.assertFalse(stage.has_finalized)

  def testHasFinalized(self):
    """Tests the has_finalized method."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertFalse(stage.has_finalized)

    other = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertFalse(other.has_finalized)

    other._context.transition_complete(other._pipeline_key)

    another = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertTrue(another.has_finalized)

  def testWasAborted(self):
    """Tests the was_aborted method."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertFalse(stage.was_aborted)

    other = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertFalse(other.was_aborted)
    other.abort()

    # Even after sending the abort signal, it won't show up as aborted.
    another = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertFalse(another.was_aborted)

    # Now transition to the aborted state.
    another._context.transition_aborted(stage._pipeline_key)
    yet_another = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertTrue(yet_another.was_aborted)

  def testRetryPossible(self):
    """Tests calling retry when it is possible."""
    stage = AsyncCancellable()
    stage.start(idempotence_key='banana')
    self.assertEquals(1, stage.current_attempt)
    self.assertTrue(stage.retry('My message 1'))

    other = AsyncCancellable.from_id(stage.pipeline_id)
    self.assertEquals(2, other.current_attempt)

    self.assertTrue(stage.retry())
    other = AsyncCancellable.from_id(stage.pipeline_id)
    self.assertEquals(3, other.current_attempt)

  def testRetryNotPossible(self):
    """Tests calling retry when the pipeline says it's not possible."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertEquals(1, stage.current_attempt)
    self.assertFalse(stage.retry())

    other = AsyncCancellable.from_id(stage.pipeline_id)
    self.assertEquals(1, other.current_attempt)

  def testRetryDisallowed(self):
    """Tests retry of the pipeline when it's not asynchronous."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertEquals(1, stage.current_attempt)
    self.assertRaises(pipeline.UnexpectedPipelineError, stage.retry)

  def testAbortRootSync(self):
    """Tests aborting a non-async, root pipeline."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    # Does not effect the current instance; it's just a signal.
    self.assertFalse(stage.was_aborted)

  def testAbortRootAsync(self):
    """Tests when the root pipeline is async and try_cancel is True."""
    stage = AsyncCancellable()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    # Does not effect the current instance; it's just a signal.
    self.assertFalse(stage.was_aborted)

  def testAbortRootAsyncNotPossible(self):
    """Tests when the root pipeline is async and cannot be canceled."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertFalse(stage.abort('gotta bail!'))
    # Does not effect the current instance; it's just a signal.
    self.assertFalse(stage.was_aborted)

  def testAbortRootSyncAlreadyAborted(self):
    """Tests aborting when the sync pipeline has already been aborted."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    self.assertFalse(stage.abort('gotta bail 2!'))

  def testAbortRootAsyncAlreadyAborted(self):
    """Tests aborting when the async pipeline has already been aborted."""
    stage = AsyncCancellable()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    self.assertFalse(stage.abort('gotta bail 2!'))

  def testFinalizeEmailDone_HighReplication(self):
    """Tests completion emails for completed root pipelines on HRD."""
    old_app_id = os.environ['APPLICATION_ID']
    testutil.TEST_APP_ID = 's~my-hrd-app'
    os.environ['APPLICATION_ID'] = testutil.TEST_APP_ID
    testutil.setup_for_testing(define_queues=['other'])
    try:
      stage = OutputlessPipeline()
      stage.start(idempotence_key='banana')
      stage._context.transition_complete(stage._pipeline_key)
      other = OutputlessPipeline.from_id(stage.pipeline_id)

      result = []
      def fake_mail(self, sender, subject, body, html=None):
        result.append((sender, subject, body, html))

      old_sendmail = pipeline.Pipeline._send_mail
      pipeline.Pipeline._send_mail = fake_mail
      try:
        other.send_result_email()
      finally:
        pipeline.Pipeline._send_mail = old_sendmail

      self.assertEquals(1, len(result))
      sender, subject, body, html = result[0]
      self.assertEquals('my-hrd-app@my-hrd-app.appspotmail.com', sender)
      self.assertEquals(
          'Pipeline successful: App "my-hrd-app", '
          '__main__.OutputlessPipeline#banana',
          subject)
      self.assertEquals(
          'View the pipeline results here:\n\n'
          'http://my-hrd-app.appspot.com/_ah/pipeline/status?root=banana\n\n'
          'Thanks,\n\nThe Pipeline API\n',
          body)
      self.assertEquals(
          '<html><body>\n<p>View the pipeline results here:</p>\n\n<p><a href="'
          'http://my-hrd-app.appspot.com/_ah/pipeline/status?root=banana"\n'
          '>http://my-hrd-app.appspot.com/_ah/pipeline/status?root=banana'
          '</a></p>\n\n<p>\nThanks,\n<br>\nThe Pipeline API\n</p>\n'
          '</body></html>\n',
          html)
    finally:
      testutil.TEST_APP_ID = old_app_id
      os.environ['APPLICATION_ID'] = old_app_id

  def testFinalizeEmailDone(self):
    """Tests completion emails for completed root pipelines."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage._context.transition_complete(stage._pipeline_key)
    other = OutputlessPipeline.from_id(stage.pipeline_id)

    result = []
    def fake_mail(self, sender, subject, body, html=None):
      result.append((sender, subject, body, html))

    old_sendmail = pipeline.Pipeline._send_mail
    pipeline.Pipeline._send_mail = fake_mail
    try:
      other.send_result_email()
    finally:
      pipeline.Pipeline._send_mail = old_sendmail

    self.assertEquals(1, len(result))
    sender, subject, body, html = result[0]
    self.assertEquals('my-app-id@my-app-id.appspotmail.com', sender)
    self.assertEquals(
        'Pipeline successful: App "my-app-id", '
        '__main__.OutputlessPipeline#banana',
        subject)
    self.assertEquals(
        'View the pipeline results here:\n\n'
        'http://my-app-id.appspot.com/_ah/pipeline/status?root=banana\n\n'
        'Thanks,\n\nThe Pipeline API\n',
        body)
    self.assertEquals(
        '<html><body>\n<p>View the pipeline results here:</p>\n\n<p><a href="'
        'http://my-app-id.appspot.com/_ah/pipeline/status?root=banana"\n'
        '>http://my-app-id.appspot.com/_ah/pipeline/status?root=banana'
        '</a></p>\n\n<p>\nThanks,\n<br>\nThe Pipeline API\n</p>\n'
        '</body></html>\n',
        html)

  def testFinalizeEmailAborted(self):
    """Tests completion emails for aborted root pipelines."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage._context.transition_aborted(stage._pipeline_key)
    other = OutputlessPipeline.from_id(stage.pipeline_id)

    result = []
    def fake_mail(self, sender, subject, body, html=None):
      result.append((sender, subject, body, html))

    old_sendmail = pipeline.Pipeline._send_mail
    pipeline.Pipeline._send_mail = fake_mail
    try:
      other.send_result_email()
    finally:
      pipeline.Pipeline._send_mail = old_sendmail

    self.assertEquals(1, len(result))
    sender, subject, body, html = result[0]
    self.assertEquals('my-app-id@my-app-id.appspotmail.com', sender)
    self.assertEquals(
        'Pipeline aborted: App "my-app-id", '
        '__main__.OutputlessPipeline#banana',
        subject)
    self.assertEquals(
        'View the pipeline results here:\n\n'
        'http://my-app-id.appspot.com/_ah/pipeline/status?root=banana\n\n'
        'Thanks,\n\nThe Pipeline API\n',
        body)
    self.assertEquals(
        '<html><body>\n<p>View the pipeline results here:</p>\n\n<p><a href="'
        'http://my-app-id.appspot.com/_ah/pipeline/status?root=banana"\n'
        '>http://my-app-id.appspot.com/_ah/pipeline/status?root=banana'
        '</a></p>\n\n<p>\nThanks,\n<br>\nThe Pipeline API\n</p>\n'
        '</body></html>\n',
        html)

  def testFinalizeEmailError(self):
    """Tests when send_result_email raises an error."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage._context.transition_complete(stage._pipeline_key)
    other = OutputlessPipeline.from_id(stage.pipeline_id)

    def fake_mail(*args, **kwargs):
      raise mail.InvalidEmailError('Doh!')

    old_sendmail = pipeline.Pipeline._send_mail
    pipeline.Pipeline._send_mail = fake_mail
    try:
      other.send_result_email()
    finally:
      pipeline.Pipeline._send_mail = old_sendmail

  def testSetStatus(self):
    """Tests for the set_status method."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage.set_status(
        message='This is my message',
        console_url='/path/to/the/console',
        status_links=dict(first='/one', second='/two', third='/three'))
    record_list = list(_StatusRecord.all())
    self.assertEquals(1, len(record_list))
    status_record = record_list[0]

    self.assertEquals('This is my message', status_record.message)
    self.assertEquals('/path/to/the/console', status_record.console_url)
    self.assertEquals(['first', 'second', 'third'], status_record.link_names)
    self.assertEquals(['/one', '/two', '/three'], status_record.link_urls)
    self.assertTrue(isinstance(status_record.status_time, datetime.datetime))

    # Now resetting it will overwrite all fields.
    stage.set_status(console_url='/another_console')
    after_status_record = db.get(status_record.key())

    self.assertEquals(None, after_status_record.message)
    self.assertEquals('/another_console', after_status_record.console_url)
    self.assertEquals([], after_status_record.link_names)
    self.assertEquals([], after_status_record.link_urls)
    self.assertNotEquals(after_status_record.status_time,
                         status_record.status_time)

  def testSetStatusError(self):
    """Tests when set_status hits a Datastore error."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    try:
      stage.set_status(message=object())
    except pipeline.PipelineRuntimeError, e:
      self.assertEquals(
          'Could not set status for __main__.OutputlessPipeline(*(), **{})'
          '#banana: Property message must be convertible to a Text instance '
          '(Text() argument should be str or unicode, not object)',
          str(e))

  def testTestMode(self):
    """Tests the test_mode property of Pipelines."""
    from pipeline import pipeline as local_pipeline
    stage = OutputlessPipeline()
    self.assertFalse(stage.test_mode)
    local_pipeline._TEST_MODE = True
    try:
      self.assertTrue(stage.test_mode)
    finally:
      local_pipeline._TEST_MODE = False

  def testCleanup(self):
    """Tests the cleanup method of Pipelines."""
    stage = OutputlessPipeline()
    self.assertRaises(pipeline.UnexpectedPipelineError, stage.cleanup)
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.is_root)
    stage.cleanup()
    task_list = test_shared.get_tasks('default')
    self.assertEquals(2, len(task_list))
    start_task, cleanup_task = task_list
    self.assertEquals('/_ah/pipeline/run', start_task['url'])

    self.assertEquals('/_ah/pipeline/cleanup', cleanup_task['url'])
    self.assertEquals(
        'aglteS1hcHAtaWRyHwsSE19BRV9QaXBlbGluZV9SZWNvcmQiBmJhbmFuYQw',
        dict(cleanup_task['headers'])['X-Ae-Pipeline-Key'])
    self.assertEquals(
        ['aglteS1hcHAtaWRyHwsSE19BRV9QaXBlbGluZV9SZWNvcmQiBmJhbmFuYQw'],
        cleanup_task['params']['root_pipeline_key'])

    # If the stage is actually a child stage, then cleanup does nothing.
    stage._root_pipeline_key = db.Key.from_path(
        _PipelineRecord.kind(), 'other')
    self.assertFalse(stage.is_root)
    stage.cleanup()
    task_list = test_shared.get_tasks('default')
    self.assertEquals(2, len(task_list))

  def testWithParams(self):
    """Tests the with_params helper method."""
    stage = OutputlessPipeline().with_params(target='my-cool-target')
    self.assertEquals('my-cool-target', stage.target)
    stage.start(idempotence_key='banana')

    task_list = test_shared.get_tasks('default')
    self.assertEquals(1, len(task_list))
    start_task = task_list[0]
    self.assertEquals('/_ah/pipeline/run', start_task['url'])
    self.assertEquals(
        'my-cool-target.my-app-id.appspot.com',
        dict(start_task['headers'])['Host'])

  def testWithParams_Errors(self):
    """Tests misuse of the with_params helper method."""
    stage = OutputlessPipeline()

    # Bad argument
    self.assertRaises(
        TypeError, stage.with_params, unknown_arg='blah')

    # If it's already active then you can't change the parameters.
    stage.start(idempotence_key='banana')
    self.assertRaises(
        pipeline.UnexpectedPipelineError, stage.with_params)


class OrderingTest(TestBase):
  """Tests for the Ordering classes."""

  def testAfterEmpty(self):
    """Tests when no futures are passed to the After() constructor."""
    pipeline.After._after_all_futures = []
    futures = []
    after = pipeline.After(*futures)
    self.assertEquals([], pipeline.After._local._after_all_futures)
    after.__enter__()
    self.assertEquals([], pipeline.After._local._after_all_futures)
    self.assertFalse(after.__exit__(None, None, None))
    self.assertEquals([], pipeline.After._local._after_all_futures)

  def testAfter(self):
    """Tests the After class."""
    pipeline.After._after_all_futures = []
    futures = [object(), object(), object()]
    after = pipeline.After(*futures)
    self.assertEquals([], pipeline.After._local._after_all_futures)
    after.__enter__()
    self.assertEquals(futures, pipeline.After._local._after_all_futures)
    self.assertFalse(after.__exit__(None, None, None))
    self.assertEquals([], pipeline.After._local._after_all_futures)

  def testAfterNested(self):
    """Tests nested behavior of the After class."""
    pipeline.After._after_all_futures = []
    futures = [object(), object(), object()]

    after = pipeline.After(*futures)
    self.assertEquals([], pipeline.After._local._after_all_futures)
    after.__enter__()
    self.assertEquals(futures, pipeline.After._local._after_all_futures)

    after2 = pipeline.After(*futures)
    self.assertEquals(futures, pipeline.After._local._after_all_futures)
    after2.__enter__()
    self.assertEquals(futures + futures,
                      pipeline.After._local._after_all_futures)

    self.assertFalse(after.__exit__(None, None, None))
    self.assertEquals(futures, pipeline.After._local._after_all_futures)
    self.assertFalse(after.__exit__(None, None, None))
    self.assertEquals([], pipeline.After._local._after_all_futures)

  def testInOrder(self):
    """Tests the InOrder class."""
    inorder = pipeline.InOrder()
    self.assertFalse(pipeline.InOrder._local._activated)
    self.assertEquals(set(), pipeline.InOrder._local._in_order_futures)
    pipeline.InOrder._add_future(object())
    self.assertEquals(set(), pipeline.InOrder._local._in_order_futures)

    inorder.__enter__()
    self.assertTrue(pipeline.InOrder._local._activated)
    one, two, three = object(), object(), object()
    pipeline.InOrder._add_future(one)
    pipeline.InOrder._add_future(two)
    pipeline.InOrder._add_future(three)
    pipeline.InOrder._add_future(three)
    self.assertEquals(set([one, two, three]),
                      pipeline.InOrder._local._in_order_futures)

    inorder.__exit__(None, None, None)
    self.assertFalse(pipeline.InOrder._local._activated)
    self.assertEquals(set(), pipeline.InOrder._local._in_order_futures)

  def testInOrderNested(self):
    """Tests nested behavior of the InOrder class."""
    inorder = pipeline.InOrder()
    self.assertFalse(pipeline.InOrder._local._activated)
    inorder.__enter__()
    self.assertTrue(pipeline.InOrder._local._activated)

    inorder2 = pipeline.InOrder()
    self.assertRaises(pipeline.UnexpectedPipelineError, inorder2.__enter__)
    inorder.__exit__(None, None, None)


class GenerateArgs(pipeline.Pipeline):
  """Pipeline to test the _generate_args helper function."""

  output_names = ['three', 'four']

  def run(self, *args, **kwargs):
    pass


class UtilitiesTest(TestBase):
  """Tests for module-level utilities."""

  def testDereferenceArgsNotFilled(self):
    """Tests when an argument was not filled."""
    slot_key = db.Key.from_path(_SlotRecord.kind(), 'myslot')
    args = [{'type': 'slot', 'slot_key': str(slot_key)}]
    self.assertRaises(pipeline.SlotNotFilledError,
        pipeline._dereference_args, 'foo', args, {})

  def testDereferenceArgsBadType(self):
    """Tests when a positional argument has a bad type."""
    self.assertRaises(pipeline.UnexpectedPipelineError,
        pipeline._dereference_args, 'foo', [{'type': 'bad'}], {})

  def testDereferenceKwargsBadType(self):
    """Tests when a keyword argument has a bad type."""
    self.assertRaises(pipeline.UnexpectedPipelineError,
        pipeline._dereference_args, 'foo', [], {'one': {'type': 'bad'}})

  def testGenerateArgs(self):
    """Tests generating a parameter dictionary from arguments."""
    future = pipeline.PipelineFuture(['one', 'two', 'unused'])
    other_future = pipeline.PipelineFuture(['three', 'four'])

    future.one.key = db.Key.from_path('First', 'one')
    future.two.key = db.Key.from_path('First', 'two')
    future.default.key = db.Key.from_path('First', 'three')
    future.unused.key = db.Key.from_path('First', 'unused')

    other_future.three.key = db.Key.from_path('Second', 'three')
    other_future.four.key = db.Key.from_path('Second', 'four')
    other_future.default.key = db.Key.from_path('Second', 'four')

    other_future._after_all_pipelines.add(future)

    # When the parameters are small.
    stage = GenerateArgs(future.one, 'some value', future,
                         red=1234, blue=future.two)
    (dependent_slots, output_slot_keys,
     params_text, params_blob) = pipeline._generate_args(
        stage,
        other_future,
        'my-queue',
        '/base-path')

    self.assertEquals(
        set([future.one.key, future.default.key, future.two.key]),
        dependent_slots)
    self.assertEquals(
        set([other_future.three.key, other_future.four.key,
             other_future.default.key]),
        output_slot_keys)

    self.assertEquals(None, params_blob)
    params = simplejson.loads(params_text)
    self.assertEquals(
        {
            'queue_name': 'my-queue',
            'after_all': [str(future.default.key)],
            'class_path': '__main__.GenerateArgs',
            'args': [
                {'slot_key': str(future.one.key),
                 'type': 'slot'},
                {'type': 'value', 'value': 'some value'},
                {'slot_key': str(future.default.key),
                 'type': 'slot'}
            ],
            'base_path': '/base-path',
            'kwargs': {
                'blue': {'slot_key': str(future.two.key),
                         'type': 'slot'},
                'red': {'type': 'value', 'value': 1234}
            },
            'output_slots': {
                'default': str(other_future.default.key),
                'four': str(other_future.four.key),
                'three': str(other_future.three.key)
            },
            'max_attempts': 3,
            'backoff_factor': 2,
            'backoff_seconds': 15,
            'task_retry': False,
            'target': None,
        }, params)

    # When the parameters are big enough we need an external blob.
    stage = GenerateArgs(future.one, 'some value' * 1000000, future,
                         red=1234, blue=future.two)

    (dependent_slots, output_slot_keys,
     params_text, params_blob) = pipeline._generate_args(
        stage,
        other_future,
        'my-queue',
        '/base-path')

    self.assertEquals(
        set([future.one.key, future.default.key, future.two.key]),
        dependent_slots)
    self.assertEquals(
        set([other_future.three.key, other_future.four.key,
             other_future.default.key]),
        output_slot_keys)

    self.assertEquals(None, params_text)
    params = simplejson.loads(blobstore.BlobInfo(params_blob).open().read())
    self.assertEquals('some value' * 1000000, params['args'][1]['value'])

  def testShortRepr(self):
    """Tests for the _short_repr function."""
    my_dict = {
      'red': 1,
      'two': ['hi'] * 100
    }
    self.assertEquals(
        "{'two': ['hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', "
        "'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', "
        "'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', "
        "'hi',... (619 bytes)",
        pipeline._short_repr(my_dict))


class PipelineContextTest(TestBase):
  """Tests for the internal _PipelineContext class."""

  def setUp(self):
    """Sets up the test harness."""
    TestBase.setUp(self)
    self.pipeline1_key = db.Key.from_path(_PipelineRecord.kind(), '1')
    self.pipeline2_key = db.Key.from_path(_PipelineRecord.kind(), '2')
    self.pipeline3_key = db.Key.from_path(_PipelineRecord.kind(), '3')
    self.pipeline4_key = db.Key.from_path(_PipelineRecord.kind(), '4')
    self.pipeline5_key = db.Key.from_path(_PipelineRecord.kind(), '5')

    self.slot1_key = db.Key.from_path(_SlotRecord.kind(), 'one')
    self.slot2_key = db.Key.from_path(_SlotRecord.kind(), 'missing')
    self.slot3_key = db.Key.from_path(_SlotRecord.kind(), 'three')
    self.slot4_key = db.Key.from_path(_SlotRecord.kind(), 'four')

    self.slot1 = _SlotRecord(
        key=self.slot1_key,
        status=_SlotRecord.FILLED)
    self.slot3 = _SlotRecord(
        key=self.slot3_key,
        status=_SlotRecord.WAITING)
    self.slot4 = _SlotRecord(
        key=self.slot4_key,
        status=_SlotRecord.FILLED)

    self.barrier1 = _BarrierRecord(
        parent=self.pipeline1_key,
        key_name=_BarrierRecord.FINALIZE,
        root_pipeline=self.pipeline1_key,
        target=self.pipeline1_key,
        blocking_slots=[self.slot1_key])
    self.barrier2 = _BarrierRecord(
        parent=self.pipeline2_key,
        key_name=_BarrierRecord.START,
        root_pipeline=self.pipeline2_key,
        target=self.pipeline2_key,
        blocking_slots=[self.slot1_key, self.slot3_key])
    self.barrier3 = _BarrierRecord(
        parent=self.pipeline3_key,
        key_name=_BarrierRecord.START,
        root_pipeline=self.pipeline3_key,
        target=self.pipeline3_key,
        blocking_slots=[self.slot1_key, self.slot4_key],
        status=_BarrierRecord.FIRED)
    self.barrier4 = _BarrierRecord(
        parent=self.pipeline4_key,
        key_name=_BarrierRecord.START,
        root_pipeline=self.pipeline4_key,
        target=self.pipeline4_key,
        blocking_slots=[self.slot1_key, self.slot2_key],
        status=_BarrierRecord.FIRED)
    self.barrier5 = _BarrierRecord(
        parent=self.pipeline5_key,
        key_name=_BarrierRecord.START,
        root_pipeline=self.pipeline5_key,
        target=self.pipeline5_key,
        blocking_slots=[self.slot1_key])

    self.context = pipeline._PipelineContext(
        'my-task1', 'default', '/base-path')

  def testNotifyBarrierFire(self):
    """Tests barrier firing behavior."""
    self.assertEquals(_BarrierRecord.WAITING, self.barrier1.status)
    self.assertEquals(_BarrierRecord.WAITING, self.barrier2.status)
    self.assertEquals(_BarrierRecord.FIRED, self.barrier3.status)
    self.assertTrue(self.barrier3.trigger_time is None)
    self.assertEquals(_BarrierRecord.FIRED, self.barrier4.status)
    self.assertEquals(_BarrierRecord.WAITING, self.barrier5.status)

    db.put([self.barrier1, self.barrier2, self.barrier3, self.barrier4,
            self.barrier5, self.slot1, self.slot3, self.slot4])
    self.context.notify_barriers(
        self.slot1_key,
        None,
        max_to_notify=3)
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(3, len(task_list))
    task_list.sort(key=lambda x: x['name'])  # For deterministic tests.
    first_task, second_task, continuation_task = task_list

    self.assertEquals(
        {'pipeline_key': [str(self.pipeline1_key)],
         'purpose': [_BarrierRecord.FINALIZE]},
        first_task['params'])
    self.assertEquals('/base-path/finalized', first_task['url'])

    self.assertEquals(
        {'pipeline_key': [str(self.pipeline3_key)],
         'purpose': [_BarrierRecord.START]},
        second_task['params'])
    self.assertEquals('/base-path/run', second_task['url'])

    self.assertEquals('/base-path/output', continuation_task['url'])
    self.assertEquals(
        [str(self.slot1_key)], continuation_task['params']['slot_key'])
    self.assertEquals(
        'my-task1-ae-barrier-notify-0',
        continuation_task['name'])

    barrier1, barrier2, barrier3 = db.get(
        [self.barrier1.key(), self.barrier2.key(), self.barrier3.key()])

    self.assertEquals(_BarrierRecord.FIRED, barrier1.status)
    self.assertTrue(barrier1.trigger_time is not None)

    self.assertEquals(_BarrierRecord.WAITING, barrier2.status)
    self.assertTrue(barrier2.trigger_time is None)

    # NOTE: This barrier relies on slots 1 and 4, to force the "blocking slots"
    # inner loop to be excerised. By putting slot4 last on the last barrier
    # tested in the loop, we ensure that any inner-loop variables do not pollute
    # the outer function context.
    self.assertEquals(_BarrierRecord.FIRED, barrier3.status)
    # Show that if the _BarrierRecord was already in the FIRED state that it
    # will not be overwritten again and have its trigger_time changed.
    self.assertTrue(barrier3.trigger_time is None)

    # Run the first continuation task.
    self.context.task_name = 'my-task1-ae-barrier-notify-0'
    self.context.notify_barriers(
        self.slot1_key,
        continuation_task['params']['cursor'][0],
        max_to_notify=2)

    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(2, len(task_list))
    third_task, continuation2_task = task_list

    self.assertEquals(
        {'pipeline_key': [str(self.pipeline5_key)],
         'purpose': [_BarrierRecord.START]},
        third_task['params'])
    self.assertEquals('/base-path/run', third_task['url'])

    self.assertEquals('/base-path/output', continuation2_task['url'])
    self.assertEquals(
        [str(self.slot1_key)], continuation2_task['params']['slot_key'])
    self.assertEquals(
        'my-task1-ae-barrier-notify-1',
        continuation2_task['name'])

    barrier4, barrier5 = db.get([self.barrier4.key(), self.barrier5.key()])
    self.assertEquals(_BarrierRecord.FIRED, barrier4.status)
    # Shows that the _BarrierRecord entity was not overwritten.
    self.assertTrue(barrier4.trigger_time is None)

    self.assertEquals(_BarrierRecord.FIRED, barrier5.status)
    self.assertTrue(barrier5.trigger_time is not None)

    # Running the continuation task again will re-tigger the barriers,
    # but no tasks will be inserted because they're already tombstoned.
    self.context.task_name = 'my-task1-ae-barrier-notify-0'
    self.context.notify_barriers(
        self.slot1_key,
        continuation_task['params']['cursor'][0],
        max_to_notify=2)
    self.assertEquals(0, len(test_shared.get_tasks()))

    # Running the last continuation task will do nothing.
    self.context.task_name = 'my-task1-ae-barrier-notify-1'
    self.context.notify_barriers(
        self.slot1_key,
        continuation2_task['params']['cursor'][0],
        max_to_notify=2)
    self.assertEquals(0, len(test_shared.get_tasks()))

  def testTransitionRunMissing(self):
    """Tests transition_run when the _PipelineRecord is missing."""
    self.assertTrue(db.get(self.pipeline1_key) is None)
    self.context.transition_run(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionRunBadStatus(self):
    """Tests transition_run when the _PipelineRecord.status is bad."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)
    self.context.transition_run(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionRunMissingBarrier(self):
    """Tests transition_run when the finalization _BarrierRecord is missing."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)
    self.assertRaises(pipeline.UnexpectedPipelineError,
        self.context.transition_run,
        self.pipeline1_key,
        blocking_slot_keys=[self.slot1_key])

  def testTransitionCompleteMissing(self):
    """Tests transition_complete when the _PipelineRecord is missing."""
    self.assertTrue(db.get(self.pipeline1_key) is None)
    self.context.transition_complete(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionCompleteBadStatus(self):
    """Tests transition_complete when the _PipelineRecord.status is bad."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)
    self.context.transition_complete(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionRetryMissing(self):
    """Tests transition_retry when the _PipelineRecord is missing."""
    self.assertTrue(db.get(self.pipeline1_key) is None)
    self.assertFalse(
        self.context.transition_retry(self.pipeline1_key, 'my message'))
    # No exception raised.
    self.assertEquals(0, len(test_shared.get_tasks()))

  def testTransitionRetryBadStatus(self):
    """Tests transition_retry when the _PipelineRecord.status is bad."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)
    self.assertFalse(
        self.context.transition_retry(self.pipeline1_key, 'my message'))
    # No exception raised.
    self.assertEquals(0, len(test_shared.get_tasks()))

  def testTransitionRetryMaxFailures(self):
    """Tests transition_retry when _PipelineRecord.max_attempts is exceeded."""
    params = {
        'backoff_seconds': 10,
        'backoff_factor': 1.5,
        'max_attempts': 15,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key,
        max_attempts=5,
        current_attempt=4,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        root_pipeline=self.pipeline5_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)
    self.assertFalse(
        self.context.transition_retry(self.pipeline1_key, 'my message'))

    # A finalize task should be enqueued.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(1, len(task_list))

    self.assertEquals('/base-path/fanout_abort', task_list[0]['url'])
    self.assertEquals(
        {'root_pipeline_key': [str(self.pipeline5_key)]},
        task_list[0]['params'])

  def testTransitionRetryTaskParams(self):
    """Tests that transition_retry will enqueue retry tasks properly.

    Attempts multiple retries and verifies ETAs and task parameters.
    """
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key,
        max_attempts=5,
        current_attempt=0,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        root_pipeline=self.pipeline5_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)

    start_time = datetime.datetime.utcnow()
    when_list = [
      start_time + datetime.timedelta(seconds=(30 * i))
      for i in xrange(5)
    ]
    closure_when_list = list(when_list)
    def fake_gettime():
      return closure_when_list.pop(0)
    self.context._gettime = fake_gettime

    for attempt, delay_seconds in enumerate([12, 18, 27, 40.5]):
      self.context.transition_retry(
          self.pipeline1_key, 'my message %d' % attempt)

      task_list = test_shared.get_tasks()
      test_shared.delete_tasks(task_list)
      self.assertEquals(1, len(task_list))
      task = task_list[0]

      self.assertEquals('/base-path/run', task['url'])
      self.assertEquals(
          {
              'pipeline_key': [str(self.pipeline1_key)],
              'attempt': [str(attempt + 1)],
              'purpose': ['start']
          }, task['params'])

      next_eta = when_list[attempt] + datetime.timedelta(seconds=delay_seconds)
      self.assertEquals(next_eta, task['eta'])

      pipeline_record = db.get(self.pipeline1_key)
      self.assertEquals(attempt + 1, pipeline_record.current_attempt)
      self.assertEquals(next_eta, pipeline_record.next_retry_time)
      self.assertEquals('my message %d' % attempt,
                        pipeline_record.retry_message)

    # Simulate last attempt.
    self.context.transition_retry(self.pipeline1_key, 'my message 5')

    # A finalize task should be enqueued.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(1, len(task_list))

    self.assertEquals('/base-path/fanout_abort', task_list[0]['url'])
    self.assertEquals(
        {'root_pipeline_key': [str(self.pipeline5_key)]},
        task_list[0]['params'])

  def testBeginAbortMissing(self):
    """Tests begin_abort when the pipeline is missing."""
    self.assertTrue(db.get(self.pipeline1_key) is None)
    self.assertFalse(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

  def testBeginAbortAlreadyAborted(self):
    """Tests begin_abort when the pipeline was already aborted."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.ABORTED,
        abort_requested=False,
        key=self.pipeline1_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params))
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)

    self.assertFalse(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

  def testBeginAbortAlreadySignalled(self):
    """Tests begin_abort when the pipeline has already been signalled."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        abort_requested=True,
        key=self.pipeline1_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params))
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)

    self.assertFalse(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

  def testBeginAbortTaskEnqueued(self):
    """Tests that a successful begin_abort will enqueue an abort task."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline1_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params))
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)

    self.assertTrue(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

    # A finalize task should be enqueued.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(1, len(task_list))

    self.assertEquals('/base-path/fanout_abort', task_list[0]['url'])
    self.assertEquals(
        {'root_pipeline_key': [str(self.pipeline1_key)]},
        task_list[0]['params'])

  def testContinueAbort(self):
    """Tests the whole life cycle of continue_abort."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record1 = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline1_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record2 = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline2_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record3 = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline3_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record4 = _PipelineRecord(
        status=_PipelineRecord.ABORTED,
        key=self.pipeline4_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record5 = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline5_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        root_pipeline=self.pipeline1_key)

    db.put([pipeline_record1, pipeline_record2, pipeline_record3,
            pipeline_record4, pipeline_record5])

    self.context.continue_abort(self.pipeline1_key, max_to_notify=2)

    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(3, len(task_list))
    # For deterministic tests.
    task_list.sort(key=lambda x: x['params'].get('pipeline_key'))
    continuation_task, first_task, second_task = task_list

    # Abort for the first pipeline
    self.assertEquals('/base-path/abort', first_task['url'])
    self.assertEquals(
        {'pipeline_key': [str(self.pipeline1_key)],
         'purpose': ['abort']},
        first_task['params'])

    # Abort for the second pipeline
    self.assertEquals('/base-path/abort', second_task['url'])
    self.assertEquals(
        {'pipeline_key': [str(self.pipeline2_key)],
         'purpose': ['abort']},
        second_task['params'])

    # Continuation
    self.assertEquals('/base-path/fanout_abort', continuation_task['url'])
    self.assertEquals(set(['cursor', 'root_pipeline_key']),
                      set(continuation_task['params'].keys()))
    self.assertEquals(str(self.pipeline1_key),
                      continuation_task['params']['root_pipeline_key'][0])
    self.assertTrue(continuation_task['name'].endswith('-0'))
    cursor = continuation_task['params']['cursor'][0]

    # Now run the continuation task
    self.context.task_name = continuation_task['name']
    self.context.continue_abort(
        self.pipeline1_key, cursor=cursor, max_to_notify=1)

    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(2, len(task_list))
    # For deterministic tests.
    task_list.sort(key=lambda x: x['params'].get('pipeline_key'))
    second_continuation_task, fifth_task = task_list

    # Abort for the third pipeline
    self.assertEquals('/base-path/abort', fifth_task['url'])
    self.assertEquals(
        {'pipeline_key': [str(self.pipeline3_key)],
         'purpose': ['abort']},
        fifth_task['params'])

    # Another continuation
    self.assertEquals('/base-path/fanout_abort',
                      second_continuation_task['url'])
    self.assertEquals(set(['cursor', 'root_pipeline_key']),
                      set(second_continuation_task['params'].keys()))
    self.assertEquals(
        str(self.pipeline1_key),
        second_continuation_task['params']['root_pipeline_key'][0])
    self.assertTrue(second_continuation_task['name'].endswith('-1'))
    cursor2 = second_continuation_task['params']['cursor'][0]

    # Now run another continuation task.
    self.context.task_name = second_continuation_task['name']
    self.context.continue_abort(
        self.pipeline1_key, cursor=cursor2, max_to_notify=2)

    # This task will find two pipelines that are already in terminal states,
    # and skip then.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(1, len(task_list))
    third_continuation_task = task_list[0]

    self.assertEquals('/base-path/fanout_abort',
                      third_continuation_task['url'])
    self.assertEquals(set(['cursor', 'root_pipeline_key']),
                      set(third_continuation_task['params'].keys()))
    self.assertEquals(
        str(self.pipeline1_key),
        third_continuation_task['params']['root_pipeline_key'][0])
    self.assertTrue(third_continuation_task['name'].endswith('-2'))
    cursor3 = third_continuation_task['params']['cursor'][0]

    # Run the third continuation task, which will do nothing.
    self.context.task_name = second_continuation_task['name']
    self.context.continue_abort(
        self.pipeline1_key, cursor=cursor3, max_to_notify=2)

    # Nothing left to do.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEquals(0, len(task_list))

  def testTransitionAbortedMissing(self):
    """Tests transition_aborted when the pipeline is missing."""
    self.assertTrue(db.get(self.pipeline1_key) is None)
    self.context.transition_aborted(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionAbortedBadStatus(self):
    """Tests transition_aborted when the pipeline is in a bad state."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    finalized_time = datetime.datetime.now()
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.ABORTED,
        key=self.pipeline1_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params),
        finalized_time=finalized_time)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)

    self.context.transition_aborted(self.pipeline1_key)

    # Finalized time will stay the same.
    after_record = db.get(self.pipeline1_key)
    self.assertEquals(pipeline_record.finalized_time,
                      after_record.finalized_time)

  def testTransitionAbortedSuccess(self):
    """Tests when transition_aborted is successful."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(params))
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline1_key) is not None)

    self.context.transition_aborted(self.pipeline1_key)

    after_record = db.get(self.pipeline1_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertTrue(pipeline_record.finalized_time is None)
    self.assertTrue(isinstance(after_record.finalized_time, datetime.datetime))


class EvaluateErrorTest(test_shared.TaskRunningMixin, TestBase):
  """Task execution tests for error situations."""

  def setUp(self):
    """Sets up the test harness."""
    super(EvaluateErrorTest, self).setUp()
    self.pipeline_key = db.Key.from_path(_PipelineRecord.kind(), '1')
    self.slot_key = db.Key.from_path(_SlotRecord.kind(), 'red')
    self.context = pipeline._PipelineContext(
        'my-task1', 'default', '/base-path')

  def testPipelineMissing(self):
    """Tests running a pipeline key that's disappeared."""
    self.assertTrue(db.get(self.pipeline_key) is None)
    self.context.evaluate(self.pipeline_key)
    # That's it. No exception raised.

  def testPipelineBadStatus(self):
    """Tests running a pipeline that has an invalid status."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.pipeline_key) is not None)
    self.context.evaluate(self.pipeline_key)

  def testDefaultSlotMissing(self):
    """Tests when the default slot is missing."""
    pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline_key,
        status=_PipelineRecord.WAITING,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps({
            'output_slots': {'default': str(self.slot_key)}}),
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.slot_key) is None)
    self.assertTrue(db.get(self.pipeline_key) is not None)
    self.context.evaluate(self.pipeline_key)
    # That's it. No exception raised.

  def testRootPipelineMissing(self):
    """Tests when the root pipeline record is missing."""
    missing_key = db.Key.from_path(_PipelineRecord.kind(), 'unknown')
    slot_record = _SlotRecord(key=self.slot_key)
    slot_record.put()
    pipeline_record = _PipelineRecord(
        root_pipeline=missing_key,
        status=_PipelineRecord.WAITING,
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps({
            'output_slots': {'default': str(self.slot_key)}}),
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(db.get(missing_key) is None)
    self.assertTrue(db.get(self.slot_key) is not None)
    self.assertTrue(db.get(self.pipeline_key) is not None)
    self.context.evaluate(self.pipeline_key)
    # That's it. No exception raised.

  def testResolutionError(self):
    """Tests when the pipeline class couldn't be found."""
    slot_record = _SlotRecord(key=self.slot_key)
    slot_record.put()
    pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline_key,
        status=_PipelineRecord.WAITING,
        class_path='does.not.exist',
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps({
            'output_slots': {'default': str(self.slot_key)}}),
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(db.get(self.slot_key) is not None)
    self.assertTrue(db.get(self.pipeline_key) is not None)
    self.assertRaises(ImportError, self.context.evaluate, self.pipeline_key)


class DumbSync(pipeline.Pipeline):
  """A dumb pipeline that's synchronous."""

  def run(self, *args):
    pass


class DumbAsync(pipeline.Pipeline):
  """A dumb pipeline that's asynchronous."""

  async = True

  def run(self):
    self.complete()


class DumbGenerator(pipeline.Pipeline):
  """A dumb pipeline that's a generator that yeilds nothing."""

  def run(self):
    if False:
      yield 1


class DumbGeneratorYields(pipeline.Pipeline):
  """A dumb pipeline that's a generator that yields something."""

  def run(self, block=False):
    yield DumbSync(1)
    result = yield DumbSync(2)
    if block:
      yield DumbSync(3, result)


class DiesOnCreation(pipeline.Pipeline):
  """A pipeline that raises an exception on insantiation."""

  def __init__(self, *args, **kwargs):
    raise Exception('This will not work!')


class DiesOnRun(pipeline.Pipeline):
  """A pipeline that raises an exception when it's executed."""

  def run(self):
    raise Exception('Cannot run this one!')


class RetryAfterYield(pipeline.Pipeline):
  """A generator pipeline that raises a Retry exception after yielding once."""

  def run(self):
    yield DumbSync()
    raise pipeline.Retry('I want to retry now!')


class DiesAfterYield(pipeline.Pipeline):
  """A generator pipeline that dies after yielding once."""

  def run(self):
    yield DumbSync()
    raise Exception('Whoops I will die now!')


class RetriesOnRun(pipeline.Pipeline):
  """A pipeline that raises a Retry exception on run."""

  def run(self):
    raise pipeline.Retry('Gotta go and retry now!')


class AbortsOnRun(pipeline.Pipeline):
  """A pipeline that raises an Abort exception on run."""

  def run(self):
    raise pipeline.Abort('Gotta go and abort now!')


class AsyncCannotAbort(pipeline.Pipeline):
  """An async pipeline that cannot be aborted once active."""

  async = True

  def run(self):
    pass


class AbortAfterYield(pipeline.Pipeline):
  """A generator pipeline that raises an Abort exception after yielding once."""

  def run(self):
    yield DumbSync()
    raise pipeline.Abort('I want to abort now!')


class AsyncCanAbort(pipeline.Pipeline):
  """An async pipeline that cannot be aborted once active."""

  async = True

  def run(self):
    pass

  def try_cancel(self):
    return True


class SyncMissedOutput(pipeline.Pipeline):
  """A sync pipeline that forgets to fill in a named output slot."""

  output_names = ['another']

  def run(self):
    return 5


class GeneratorMissedOutput(pipeline.Pipeline):
  """A generator pipeline that forgets to fill in a named output slot."""

  output_names = ['another']

  def run(self):
    if False:
      yield 1


class TaskRunningTest(test_shared.TaskRunningMixin, TestBase):
  """End-to-end tests for task-running and race-condition situations.

  Many of these are cases where an executor task runs for a second time when
  it shouldn't have or some kind of transient error occurred.
  """

  def setUp(self):
    """Sets up the test harness."""
    super(TaskRunningTest, self).setUp()
    self.pipeline_key = db.Key.from_path(_PipelineRecord.kind(), 'one')
    self.pipeline2_key = db.Key.from_path(_PipelineRecord.kind(), 'two')
    self.slot_key = db.Key.from_path(_SlotRecord.kind(), 'red')

    self.slot_record = _SlotRecord(key=self.slot_key)
    self.pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline_key,
        status=_PipelineRecord.WAITING,
        class_path='does.not.exist',
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps({
                 'output_slots': {'default': str(self.slot_key)},
                 'args': [],
                 'kwargs': {},
                 'task_retry': False,
                 'backoff_seconds': 1,
                 'backoff_factor': 2,
                 'max_attempts': 4,
                 'queue_name': 'default',
                 'base_path': '',
               }),
        key=self.pipeline_key,
        max_attempts=4)
    self.barrier_record = _BarrierRecord(
            parent=self.pipeline_key,
            key_name=_BarrierRecord.FINALIZE,
            target=self.pipeline_key,
            root_pipeline=self.pipeline_key,
            blocking_slots=[self.slot_key])

    self.context = pipeline._PipelineContext(
        'my-task1', 'default', '/base-path')

  def testSubstagesRunImmediately(self):
    """Tests that sub-stages with no blocking slots are run immediately."""
    self.pipeline_record.class_path = '__main__.DumbGeneratorYields'
    db.put([self.pipeline_record, self.slot_record, self.barrier_record])

    before_record = db.get(self.pipeline_key)
    self.assertEquals([], before_record.fanned_out)

    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(2, len(after_record.fanned_out))
    child1_key, child2_key = after_record.fanned_out

    task_list = test_shared.get_tasks()
    self.assertEquals(1, len(task_list))
    fanout_task = task_list[0]

    # Verify that the start time is set for non-blocked child pipelines.
    child_record_list = db.get(after_record.fanned_out)
    for child_record in child_record_list:
      self.assertTrue(child_record.start_time is not None)

    # One fan-out task with both children.
    self.assertEquals(
        [str(self.pipeline_key)],
        fanout_task['params']['parent_key'])
    self.assertEquals(
        ['0', '1'],
        fanout_task['params']['child_indexes'])
    self.assertEquals('/base-path/fanout', fanout_task['url'])

    # Only finalization barriers present.
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.START,
                         parent=child1_key)) is None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.START,
                         parent=child2_key)) is None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.FINALIZE,
                         parent=child1_key)) is not None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.FINALIZE,
                         parent=child2_key)) is not None)

  def testSubstagesBlock(self):
    """Tests that sub-stages with pending inputs will have a barrier added."""
    self.pipeline_record.class_path = '__main__.DumbGeneratorYields'
    params = self.pipeline_record.params.copy()
    params.update({
        'output_slots': {'default': str(self.slot_key)},
        'args': [{'type': 'value', 'value': True}],
        'kwargs': {},
    })
    self.pipeline_record.params_text = simplejson.dumps(params)
    db.put([self.pipeline_record, self.slot_record, self.barrier_record])

    before_record = db.get(self.pipeline_key)
    self.assertEquals([], before_record.fanned_out)

    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(3, len(after_record.fanned_out))

    task_list = test_shared.get_tasks()
    self.assertEquals(1, len(task_list))
    fanout_task = task_list[0]

    # Only two children should start.
    self.assertEquals('/base-path/fanout', fanout_task['url'])
    self.assertEquals(
        [str(self.pipeline_key)],
        fanout_task['params']['parent_key'])
    self.assertEquals(
        ['0', '1'],
        fanout_task['params']['child_indexes'])

    run_children = set(after_record.fanned_out[int(i)]
                       for i in fanout_task['params']['child_indexes'])
    self.assertEquals(2, len(run_children))
    child1_key, child2_key = run_children
    other_child_key = list(set(after_record.fanned_out) - run_children)[0]

    # Only a start barrier inserted for the one pending child.
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.START,
                         parent=child1_key)) is None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.START,
                         parent=child2_key)) is None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.START,
                         parent=other_child_key)) is not None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.FINALIZE,
                         parent=child1_key)) is not None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.FINALIZE,
                         parent=child2_key)) is not None)
    self.assertTrue(db.get(
        db.Key.from_path(_BarrierRecord.kind(), _BarrierRecord.FINALIZE,
                         parent=other_child_key)) is not None)

  def testFannedOutOrdering(self):
    """Tests that the fanned_out property lists children in code order."""
    self.pipeline_record.class_path = '__main__.DumbGeneratorYields'
    params = self.pipeline_record.params.copy()
    params.update({
        'output_slots': {'default': str(self.slot_key)},
        'args': [{'type': 'value', 'value': True}],
        'kwargs': {},
    })
    self.pipeline_record.params_text = simplejson.dumps(params)
    db.put([self.pipeline_record, self.slot_record, self.barrier_record])

    before_record = db.get(self.pipeline_key)
    self.assertEquals([], before_record.fanned_out)

    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(3, len(after_record.fanned_out))

    children = db.get(after_record.fanned_out)
    self.assertEquals(1, children[0].params['args'][0]['value'])
    self.assertEquals(2, children[1].params['args'][0]['value'])
    self.assertEquals(3, children[2].params['args'][0]['value'])

  def testSyncWaitingStartRerun(self):
    """Tests a waiting, sync pipeline being re-run after it already output."""
    self.pipeline_record.class_path = '__main__.DumbSync'
    db.put([self.pipeline_record, self.slot_record])

    before_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.WAITING, before_record.status)
    self.assertTrue(before_record.fill_time is None)
    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, after_record.status)
    self.assertTrue(after_record.fill_time is not None)

    after_pipeline = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_pipeline.status)

    self.context.evaluate(self.pipeline_key)
    second_after_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, second_after_record.status)
    self.assertTrue(second_after_record.fill_time is not None)

    # The output slot fill times are different, which means the pipeline re-ran.
    self.assertNotEquals(second_after_record.fill_time, after_record.fill_time)

  def testSyncFinalizingRerun(self):
    """Tests a finalizing, sync pipeline task being re-run."""
    self.pipeline_record.class_path = '__main__.DumbSync'
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = simplejson.dumps(None)
    db.put([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    second_after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    # Finalized time will stay the same.
    self.assertEquals(after_record.finalized_time,
                      second_after_record.finalized_time)

  def testSyncDoneFinalizeRerun(self):
    """Tests a done, sync pipeline task being re-refinalized."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '__main__.DumbSync'
    self.pipeline_record.status = _PipelineRecord.DONE
    self.pipeline_record.finalized_time = now
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = simplejson.dumps(None)
    db.put([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    # Finalize time stays the same.
    self.assertEquals(now, after_record.finalized_time)

  def testAsyncWaitingRerun(self):
    """Tests a waiting, async pipeline task being re-run."""
    self.pipeline_record.class_path = '__main__.DumbAsync'
    db.put([self.pipeline_record, self.slot_record])

    before_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.WAITING, before_record.status)
    self.assertTrue(before_record.fill_time is None)
    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, after_record.status)
    self.assertTrue(after_record.fill_time is not None)

    after_pipeline = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.RUN, after_pipeline.status)

    self.context.evaluate(self.pipeline_key)
    second_after_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, second_after_record.status)
    self.assertTrue(second_after_record.fill_time is not None)

    # The output slot fill times are different, which means the pipeline re-ran.
    self.assertNotEquals(second_after_record.fill_time, after_record.fill_time)

  def testAsyncRunRerun(self):
    """Tests a run, async pipeline task being re-run."""
    self.pipeline_record.class_path = '__main__.DumbAsync'
    self.pipeline_record.status = _PipelineRecord.RUN
    db.put([self.pipeline_record, self.slot_record])

    before_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.WAITING, before_record.status)
    self.assertTrue(before_record.fill_time is None)
    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, after_record.status)
    self.assertTrue(after_record.fill_time is not None)

    after_pipeline = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.RUN, after_pipeline.status)

    self.context.evaluate(self.pipeline_key)
    second_after_record = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, second_after_record.status)
    self.assertTrue(second_after_record.fill_time is not None)

    # The output slot fill times are different, which means the pipeline re-ran.
    self.assertNotEquals(second_after_record.fill_time, after_record.fill_time)

  def testAsyncFinalizingRerun(self):
    """Tests a finalizing, async pipeline task being re-run."""
    self.pipeline_record.class_path = '__main__.DumbAsync'
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = simplejson.dumps(None)
    db.put([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    after_pipeline = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_pipeline.status)

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    second_after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    # Finalized time will stay the same.
    self.assertEquals(after_record.finalized_time,
                      second_after_record.finalized_time)

  def testAsyncDoneFinalizeRerun(self):
    """Tests a done, async pipeline task being re-finalized."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '__main__.DumbAsync'
    self.pipeline_record.status = _PipelineRecord.DONE
    self.pipeline_record.finalized_time = now
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = simplejson.dumps(None)
    db.put([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    # Finalize time stays the same.
    self.assertEquals(now, after_record.finalized_time)

  def testNonYieldingGeneratorWaitingFilled(self):
    """Tests a waiting, non-yielding generator will fill its output slot."""
    self.pipeline_record.class_path = '__main__.DumbGenerator'
    db.put([self.pipeline_record, self.slot_record])

    self.assertEquals(_SlotRecord.WAITING, db.get(self.slot_key).status)
    self.context.evaluate(self.pipeline_key)

    # Output slot is filled.
    after_slot = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, after_slot.status)

    # Pipeline is now in the run state.
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.RUN, after_record.status)

  def testNonYieldingGeneratorRunNotFilledRerun(self):
    """Tests a run, non-yielding generator with a not filled output slot.

    This happens when the generator yields no children and is moved to the
    RUN state but then fails before it could output to the default slot.
    """
    self.pipeline_record.class_path = '__main__.DumbGenerator'
    self.pipeline_record.status = _PipelineRecord.RUN
    db.put([self.pipeline_record, self.slot_record])

    self.assertEquals(_SlotRecord.WAITING, db.get(self.slot_key).status)
    self.context.evaluate(self.pipeline_key)

    # Output slot is filled.
    after_slot = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.FILLED, after_slot.status)

  def testGeneratorRunReRun(self):
    """Tests a run, yielding generator that is re-run."""
    self.pipeline_record.class_path = '__main__.DumbGeneratorYields'
    self.pipeline_record.status = _PipelineRecord.RUN
    self.pipeline_record.fanned_out = [self.pipeline2_key]
    db.put([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key)
    # Output slot wasn't filled.
    after_slot = db.get(self.slot_key)
    self.assertEquals(_SlotRecord.WAITING, after_slot.status)

    # Status hasn't changed.
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.RUN, after_record.status)

  def testGeneratorFinalizingRerun(self):
    """Tests a finalizing, generator pipeline task being re-run."""
    self.pipeline_record.class_path = '__main__.DumbGeneratorYields'
    self.pipeline_record.status = _PipelineRecord.RUN
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = simplejson.dumps(None)
    db.put([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    second_after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    # Finalized time will stay the same.
    self.assertEquals(after_record.finalized_time,
                      second_after_record.finalized_time)

  def testGeneratorDoneFinalizeRerun(self):
    """Tests a done, generator pipeline task being re-run."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '__main__.DumbGeneratorYields'
    self.pipeline_record.status = _PipelineRecord.DONE
    self.pipeline_record.finalized_time = now
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = simplejson.dumps(None)
    db.put([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.DONE, after_record.status)

    # Finalize time stays the same.
    self.assertEquals(now, after_record.finalized_time)

  def testFromIdFails(self):
    """Tests when evaluate's call to from_id fails a retry attempt is made."""
    self.pipeline_record.class_path = '__main__.DiesOnCreation'
    db.put([self.pipeline_record, self.slot_record])
    self.assertEquals(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals('Exception: This will not work!',
                      after_record.retry_message)

  def testMismatchedAttempt(self):
    """Tests when the task's current attempt does not match the datastore."""
    self.pipeline_record.class_path = '__main__.DiesOnRun'
    self.pipeline_record.current_attempt = 3
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key,
                          purpose=_BarrierRecord.START,
                          attempt=1)

    # Didn't run because no state change occurred, retry count is the same.
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(3, after_record.current_attempt)
    self.assertEquals(None, after_record.retry_message)

  def testPastMaxAttempts(self):
    """Tests when the current attempt number is beyond the max attempts.

    This could happen if the user edits 'max_attempts' during execution.
    """
    self.pipeline_record.class_path = '__main__.DiesOnRun'
    self.pipeline_record.current_attempt = 5
    self.pipeline_record.max_attempts = 3
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key,
                          purpose=_BarrierRecord.START,
                          attempt=5)

    # Didn't run because no state change occurred, retry count is the same.
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(5, after_record.current_attempt)
    self.assertEquals(None, after_record.retry_message)

  def testPrematureRetry(self):
    """Tests when the current retry request came prematurely."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '__main__.DiesOnRun'
    self.pipeline_record.current_attempt = 1
    self.pipeline_record.max_attempts = 3
    self.pipeline_record.next_retry_time = now + datetime.timedelta(seconds=30)
    db.put([self.pipeline_record, self.slot_record])

    self.assertRaises(
        pipeline.UnexpectedPipelineError,
        self.context.evaluate,
        self.pipeline_key,
        purpose=_BarrierRecord.START,
        attempt=1)

    # Didn't run because no state change occurred, retry count is the same.
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals(None, after_record.retry_message)

  def testRunExceptionRetry(self):
    """Tests that exceptions in Sync/Async pipelines cause a retry."""
    self.pipeline_record.class_path = '__main__.DiesOnRun'
    db.put([self.pipeline_record, self.slot_record])
    self.assertEquals(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals('Exception: Cannot run this one!',
                      after_record.retry_message)

  def testRunForceRetry(self):
    """Tests that explicit Retry on a synchronous pipeline."""
    self.pipeline_record.class_path = '__main__.RetriesOnRun'
    db.put([self.pipeline_record, self.slot_record])
    self.assertEquals(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals('Gotta go and retry now!',
                      after_record.retry_message)

  def testGeneratorExceptionRetry(self):
    """Tests that exceptions in a generator pipeline cause a retry."""
    self.pipeline_record.class_path = '__main__.DiesAfterYield'
    db.put([self.pipeline_record, self.slot_record])
    self.assertEquals(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals('Exception: Whoops I will die now!',
                      after_record.retry_message)

  def testGeneratorForceRetry(self):
    """Tests when a generator raises a user-initiated retry exception."""
    self.pipeline_record.class_path = '__main__.RetryAfterYield'
    db.put([self.pipeline_record, self.slot_record])
    self.assertEquals(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals('I want to retry now!', after_record.retry_message)

  def testNonAsyncAbortSignal(self):
    """Tests when a non-async pipeline receives the abort signal."""
    self.pipeline_record.class_path = '__main__.DumbSync'
    self.pipeline_record.status = _PipelineRecord.WAITING
    self.assertTrue(self.pipeline_record.finalized_time is None)
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertEquals(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testAbortRootPipelineFastPath(self):
    """Tests root pipeline status also functions as the abort signal."""
    root_pipeline = _PipelineRecord(
        root_pipeline=self.pipeline2_key,
        status=_PipelineRecord.RUN,
        class_path='does.not.exist',
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps({
                 'output_slots': {'default': str(self.slot_key)},
                 'args': [],
                 'kwargs': {},
                 'task_retry': False,
                 'backoff_seconds': 1,
                 'backoff_factor': 2,
                 'max_attempts': 4,
                 'queue_name': 'default',
                 'base_path': '',
               }),
        key=self.pipeline2_key,
        is_root_pipeline=True,
        max_attempts=4,
        abort_requested=True)

    # Use DiesOnRun to ensure that we don't actually run the pipeline.
    self.pipeline_record.class_path = '__main__.DiesOnRun'
    self.pipeline_record.root_pipeline = self.pipeline2_key

    db.put([self.pipeline_record, self.slot_record, root_pipeline])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertEquals(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testNonAsyncAbortSignalRepeated(self):
    """Tests when a non-async pipeline has the abort request repeated.

    Tests the case of getting the abort signal is successful, and that the
    pipeline will finalize before being aborted.
    """
    self.pipeline_record.class_path = '__main__.DumbSync'
    self.pipeline_record.status = _PipelineRecord.WAITING
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertEquals(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

    # Run a second time-- this should be ignored.
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)
    after_record2 = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertEquals(0, after_record2.current_attempt)
    self.assertTrue(after_record2.retry_message is None)
    self.assertTrue(after_record2.abort_message is None)
    self.assertEquals(after_record.finalized_time, after_record2.finalized_time)

  def testAsyncAbortSignalBeforeStart(self):
    """Tests when an async pipeline has an abort request and has not run yet.

    Verifies that the pipeline will be finalized and transitioned to ABORTED.
    """
    self.pipeline_record.class_path = '__main__.DumbAsync'
    self.pipeline_record.status = _PipelineRecord.WAITING
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertEquals(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testAsyncAbortSignalDisallowed(self):
    """Tests when an async pipeline receives abort but try_cancel is False."""
    self.pipeline_record.class_path = '__main__.AsyncCannotAbort'
    self.pipeline_record.status = _PipelineRecord.RUN
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.RUN, after_record.status)
    self.assertEquals(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is None)

  def testAsyncAbortSignalAllowed(self):
    """Tests when an async pipeline receives abort but try_cancel is True."""
    self.pipeline_record.class_path = '__main__.AsyncCanAbort'
    self.pipeline_record.status = _PipelineRecord.RUN
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertEquals(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testGeneratorAbortException(self):
    """Tests when a generator raises an abort after it's begun yielding."""
    self.pipeline_record.class_path = '__main__.AbortAfterYield'
    self.pipeline_record.status = _PipelineRecord.RUN
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)
    self.assertEquals(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testRetryWhenSyncDoesNotFillSlot(self):
    """Tests when a sync pipeline does not fill a slot that it will retry."""
    self.pipeline_record.class_path = '__main__.SyncMissedOutput'
    self.pipeline_record.status = _PipelineRecord.WAITING
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals(
        'SlotNotFilledError: Outputs set([\'another\']) for pipeline ID "one" '
        'were never filled by "__main__.SyncMissedOutput".',
        after_record.retry_message)

  def testNonYieldingGeneratorDoesNotFillSlot(self):
    """Tests non-yielding pipelines that do not fill a slot will retry."""
    self.pipeline_record.class_path = '__main__.GeneratorMissedOutput'
    self.pipeline_record.status = _PipelineRecord.WAITING
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key)

    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.WAITING, after_record.status)
    self.assertEquals(1, after_record.current_attempt)
    self.assertEquals(
        'SlotNotFilledError: Outputs set([\'another\']) for pipeline ID "one" '
        'were never filled by "__main__.GeneratorMissedOutput".',
        after_record.retry_message)

  def testAbortWithBadInputs(self):
    """Tests aborting a pipeline with unresolvable input slots."""
    self.pipeline_record.class_path = '__main__.DumbSync'
    self.pipeline_record.params['args'] = [
        {'type': 'slot',
         'slot_key': 'aglteS1hcHAtaWRyGQsSEF9BRV9DYXNjYWRlX1Nsb3QiA3JlZAw'}
    ]
    self.pipeline_record.status = _PipelineRecord.WAITING
    db.put([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    # Forced into the abort state.
    after_record = db.get(self.pipeline_key)
    self.assertEquals(_PipelineRecord.ABORTED, after_record.status)


class HandlersPrivateTest(TestBase):
  """Tests that the pipeline request handlers are all private."""

  def testBarrierHandler(self):
    """Tests the _BarrierHandler."""
    handler = test_shared.create_handler(pipeline._BarrierHandler, 'POST', '/')
    handler.post()
    self.assertEquals((403, 'Forbidden'), handler.response._Response__status)

  def testPipelineHandler(self):
    """Tests the _PipelineHandler."""
    handler = test_shared.create_handler(pipeline._PipelineHandler, 'POST', '/')
    handler.post()
    self.assertEquals((403, 'Forbidden'), handler.response._Response__status)

  def testFanoutAbortHandler(self):
    """Tests the _FanoutAbortHandler."""
    handler = test_shared.create_handler(
        pipeline._FanoutAbortHandler, 'POST', '/')
    handler.post()
    self.assertEquals((403, 'Forbidden'), handler.response._Response__status)

  def testFanoutHandler(self):
    """Tests the _FanoutHandler."""
    handler = test_shared.create_handler(pipeline._FanoutHandler, 'POST', '/')
    handler.post()
    self.assertEquals((403, 'Forbidden'), handler.response._Response__status)

  def testCleanupHandler(self):
    """Tests the _CleanupHandler."""
    handler = test_shared.create_handler(pipeline._CleanupHandler, 'POST', '/')
    handler.post()
    self.assertEquals((403, 'Forbidden'), handler.response._Response__status)


class InternalOnlyPipeline(pipeline.Pipeline):
  """Pipeline with internal-only callbacks."""

  async = True

  def run(self):
    pass


class AdminOnlyPipeline(pipeline.Pipeline):
  """Pipeline with internal-only callbacks."""

  async = True
  admin_callbacks = True

  def run(self):
    pass

  def callback(self, **kwargs):
    pass


class PublicPipeline(pipeline.Pipeline):
  """Pipeline with public callbacks."""

  async = True
  public_callbacks = True

  def run(self):
    pass

  def callback(self, **kwargs):
    return (200, 'text/plain', repr(kwargs))


class CallbackHandlerTest(TestBase):
  """Tests for the _CallbackHandler class."""

  def testErrors(self):
    """Tests for error conditions."""
    # No pipeline_id param.
    handler = test_shared.create_handler(
        pipeline._CallbackHandler, 'GET', '/?red=one&blue=two')
    handler.get()
    self.assertEquals((400, 'Bad Request'), handler.response._Response__status)

    # Non-existent pipeline.
    handler = test_shared.create_handler(
        pipeline._CallbackHandler, 'GET', '/?pipeline_id=blah&red=one&blue=two')
    handler.get()
    self.assertEquals((400, 'Bad Request'), handler.response._Response__status)

    # Pipeline exists but class path is bogus.
    stage = InternalOnlyPipeline()
    stage.start()

    pipeline_record = pipeline.models._PipelineRecord.get_by_key_name(
        stage.pipeline_id)
    params = pipeline_record.params
    params['class_path'] = 'does.not.exist'
    pipeline_record.params_text = simplejson.dumps(params)
    pipeline_record.put()

    handler = test_shared.create_handler(
        pipeline._CallbackHandler,
        'GET', '/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    handler.get()
    self.assertEquals((400, 'Bad Request'), handler.response._Response__status)

    # Internal-only callbacks.
    stage = InternalOnlyPipeline()
    stage.start()
    handler = test_shared.create_handler(
        pipeline._CallbackHandler,
        'GET', '/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    handler.get()
    self.assertEquals((400, 'Bad Request'), handler.response._Response__status)

    # Admin-only callbacks but not admin.
    stage = AdminOnlyPipeline()
    stage.start()
    handler = test_shared.create_handler(
        pipeline._CallbackHandler,
        'GET', '/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    handler.get()
    self.assertEquals((400, 'Bad Request'), handler.response._Response__status)

  def testAdminOnly(self):
    """Tests accessing a callback that is admin-only."""
    stage = AdminOnlyPipeline()
    stage.start()

    os.environ['USER_IS_ADMIN'] = '1'
    try:
      handler = test_shared.create_handler(
          pipeline._CallbackHandler,
          'GET', '/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
      handler.get()
    finally:
      del os.environ['USER_IS_ADMIN']

    self.assertEquals((200, 'OK'), handler.response._Response__status)

  def testPublic(self):
    """Tests accessing a callback that is public."""
    stage = PublicPipeline()
    stage.start()
    handler = test_shared.create_handler(
        pipeline._CallbackHandler,
        'GET', '/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    handler.get()
    self.assertEquals((200, 'OK'), handler.response._Response__status)

  def testReturnValue(self):
    """Tests when the callback has a return value to render as output."""
    stage = PublicPipeline()
    stage.start()
    handler = test_shared.create_handler(
        pipeline._CallbackHandler,
        'GET', '/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    handler.get()
    self.assertEquals((200, 'OK'), handler.response._Response__status)
    self.assertEquals(
        "{'blue': u'two', 'red': u'one'}",
        handler.response.out.getvalue())
    self.assertEquals('text/plain', handler.response.headers['Content-Type'])


class CleanupHandlerTest(test_shared.TaskRunningMixin, TestBase):
  """Tests for the _CleanupHandler class."""

  def testSuccess(self):
    """Tests successfully deleting all child pipeline elements."""
    self.assertEquals(0, len(list(_PipelineRecord.all())))
    self.assertEquals(0, len(list(_SlotRecord.all())))
    self.assertEquals(0, len(list(_BarrierRecord.all())))
    self.assertEquals(0, len(list(_StatusRecord.all())))

    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage.set_status('My status here!')
    self.assertEquals(1, len(list(_PipelineRecord.all())))
    self.assertEquals(1, len(list(_SlotRecord.all())))
    self.assertEquals(1, len(list(_BarrierRecord.all())))
    self.assertEquals(1, len(list(_StatusRecord.all())))

    stage.cleanup()
    task_list = self.get_tasks()
    self.assertEquals(2, len(task_list))

    # The order of the tasks (start or cleanup) is unclear, so
    # fish out the one that's the cleanup task and run it directly.
    for task in task_list:
      if task['url'] == '/_ah/pipeline/cleanup':
        self.run_task(task)

    self.assertEquals(0, len(list(_PipelineRecord.all())))
    self.assertEquals(0, len(list(_SlotRecord.all())))
    self.assertEquals(0, len(list(_BarrierRecord.all())))
    self.assertEquals(0, len(list(_StatusRecord.all())))


class FanoutHandlerTest(test_shared.TaskRunningMixin, TestBase):
  """Tests for the _FanoutHandler class."""

  def testOldStyle(self):
    """Tests the old fanout parameter style for backwards compatibility."""
    stage = DumbGeneratorYields()
    stage.start(idempotence_key='banana')
    task_list = self.get_tasks()
    test_shared.delete_tasks(task_list)
    self.run_task(task_list[0])

    task_list = self.get_tasks()
    self.assertEquals(1, len(task_list))
    fanout_task = task_list[0]
    self.assertEquals('/_ah/pipeline/fanout', fanout_task['url'])

    after_record = db.get(stage._pipeline_key)

    fanout_task['body'] = base64.b64encode(urllib.urlencode(
        [('pipeline_key', str(after_record.fanned_out[0])),
         ('pipeline_key', str(after_record.fanned_out[1]))]))
    test_shared.delete_tasks(task_list)
    self.run_task(fanout_task)

    task_list = self.get_tasks()
    test_shared.delete_tasks(task_list)

    self.assertEquals(2, len(task_list))
    for task in task_list:
      self.assertEquals('/_ah/pipeline/run', task['url'])
    children_keys = [
        db.Key(t['params']['pipeline_key'][0]) for t in task_list]

    self.assertEquals(set(children_keys), set(after_record.fanned_out))


################################################################################
# Begin functional test section!

class RunOrder(db.Model):
  """Saves the order of method calls."""

  order = db.ListProperty(db.Text)

  @classmethod
  def add(cls, message):
    def txn():
      runorder = RunOrder.get_by_key_name('singleton')
      if runorder is None:
        runorder = RunOrder(key_name='singleton')
      runorder.order.append(db.Text(message))
      runorder.put()
    db.run_in_transaction(txn)

  @classmethod
  def get(cls):
    runorder = RunOrder.get_by_key_name('singleton')
    if runorder is None:
      return []
    else:
      return [str(s) for s in runorder.order]


class SaveRunOrder(pipeline.Pipeline):
  """Pipeline that saves the run order message supplied."""

  def run(self, message):
    RunOrder.add(message)


class EchoSync(pipeline.Pipeline):
  """Pipeline that echos input."""

  def run(self, *args):
    if not args:
      return None
    if len(args) == 1:
      return args[0]
    return args


class EchoAsync(pipeline.Pipeline):
  """Asynchronous pipeline that echos input."""

  async = True

  def run(self, *args):
    self.get_callback_task(
        params=dict(return_value=pickle.dumps(args))).add()

  def callback(self, return_value):
    args = pickle.loads(str(return_value))
    if not args:
      self.complete(None)
    elif len(args) == 1:
      self.complete(args[0])
    else:
      self.complete(args)

  def run_test(self, *args):
    self.callback(pickle.dumps(args))


class EchoNamedSync(pipeline.Pipeline):
  """Pipeline that echos named inputs to named outputs."""

  def run(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in kwargs.iteritems():
      self.fill(name, prefix + value)


class EchoParticularNamedSync(EchoNamedSync):
  """Has preexisting output names so it can be used as a root pipeline."""

  output_names = ['one', 'two', 'three', 'four']


class EchoNamedAsync(pipeline.Pipeline):
  """Asynchronous pipeline that echos named inputs to named outputs."""

  async = True

  def run(self, **kwargs):
    self.get_callback_task(params=kwargs).add()

  def callback(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in kwargs.iteritems():
      self.fill(name, prefix + value)
    self.complete()

  def run_test(self, **kwargs):
    self.callback(**kwargs)


class EchoNamedHalfAsync(pipeline.Pipeline):
  """Pipeline that echos to named outputs and completes async.

  This is different than the other EchoNamedAsync because it fills all the
  slots except the default slot immediately, and then uses a callback to
  finally complete.
  """

  async = True
  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in kwargs.iteritems():
      self.fill(name, prefix + value)
    self.get_callback_task(params=kwargs).add()

  def callback(self, **kwargs):
    self.complete()

  def run_test(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in kwargs.iteritems():
      self.fill(name, prefix + value)
    self.callback(**kwargs)


class EchoParticularNamedAsync(EchoNamedAsync):
  """Has preexisting output names so it can be used as a root pipeline."""

  output_names = ['one', 'two', 'three', 'four']


class FillAndPass(pipeline.Pipeline):
  """Test pipeline that fills some outputs and passes the rest to a child."""

  def run(self, to_fill, **kwargs):
    for name in to_fill:
      self.fill(name, kwargs.pop(name))
    adjusted_kwargs = {}
    for name, value in kwargs.iteritems():
      adjusted_kwargs[name] = value
    if adjusted_kwargs:
      yield EchoNamedSync(**adjusted_kwargs)


class FillAndPassParticular(FillAndPass):
  """Has preexisting output names so it can be used as a root pipeline."""

  output_names = ['one', 'two', 'three', 'four']


class StrictChildInheritsAll(pipeline.Pipeline):
  """Test pipeline whose strict child inherits all outputs."""

  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    yield EchoParticularNamedSync(**kwargs)


class StrictChildGeneratorInheritsAll(pipeline.Pipeline):
  """Test pipeline whose strict child generator inherits all outputs."""

  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    yield FillAndPassParticular(kwargs.keys(), **kwargs)


class ConsumePartialChildrenStrict(pipeline.Pipeline):
  """Test pipeline that consumes a subset of a strict child's outputs."""

  def run(self, **kwargs):
    result = yield EchoParticularNamedSync(**kwargs)
    yield EchoSync(result.one, result.two)


class ConsumePartialChildren(pipeline.Pipeline):
  """Test pipeline that consumes a subset of a dynamic child's outputs."""

  def run(self, **kwargs):
    result = yield EchoNamedSync(**kwargs)
    yield EchoSync(result.one, result.two)


class DoNotConsumeDefault(pipeline.Pipeline):
  """Test pipeline that does not consume a child's default output."""

  def run(self, value):
    yield EchoSync('not used')
    yield EchoSync(value)


class TwoLevelFillAndPass(pipeline.Pipeline):
  """Two-level deep version of fill and pass."""

  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    # This stage will prefix any keyword args with 'first-'.
    stage = yield FillAndPass(
        [],
        prefix='first-',
        one=kwargs.get('one'),
        two=kwargs.get('two'))
    adjusted_kwargs = kwargs.copy()
    adjusted_kwargs['one'] = stage.one
    adjusted_kwargs['two'] = stage.two
    adjusted_kwargs['prefix'] = 'second-'
    # This stage will prefix any keyword args with 'second-'. That means
    # any args that were passed in from the output of the first stage will
    # be prefixed twice: 'second-first-<kwarg>'.
    yield FillAndPass([], **adjusted_kwargs)


class DivideWithRemainder(pipeline.Pipeline):
  """Divides a number, returning the divisor and the quotient."""

  output_names = ['remainder']

  def run(self, dividend, divisor):
    self.fill(self.outputs.remainder, dividend % divisor)
    return dividend // divisor


class EuclidGCD(pipeline.Pipeline):
  """Does the Euclidean Greatest Common Factor recursive algorithm."""

  output_names = ['gcd']

  def run(self, a, b):
    a, b = max(a, b), min(a, b)
    if b == 0:
      self.fill(self.outputs.gcd, a)
      return
    result = yield DivideWithRemainder(a, b)
    recurse = yield EuclidGCD(b, result.remainder)


class UnusedOutputReference(pipeline.Pipeline):
  """Test pipeline that touches a child output but doesn't consume it."""

  def run(self):
    result = yield EchoParticularNamedSync(
        one='red', two='blue', three='green', four='yellow')
    print result.one
    print result.two
    print result.three
    yield EchoSync(result.four)


class AccessUndeclaredDefaultOnly(pipeline.Pipeline):
  """Test pipeline accesses undeclared output of a default-only pipeline."""

  def run(self):
    result = yield EchoSync('hi')
    yield EchoSync(result.does_not_exist)


class RunMethod(pipeline.Pipeline):
  """Test pipeline that outputs what method was used for running it."""

  def run(self):
    return 'run()'

  def run_test(self):
    return 'run_test()'


class DoAfter(pipeline.Pipeline):
  """Test the After clause."""

  def run(self):
    first = yield SaveRunOrder('first')
    second = yield SaveRunOrder('first')

    with pipeline.After(first, second):
      third = yield SaveRunOrder('third')
      fourth = yield SaveRunOrder('third')


class DoAfterNested(pipeline.Pipeline):
  """Test the After clause in multiple nestings."""

  def run(self):
    first = yield SaveRunOrder('first')
    second = yield SaveRunOrder('first')

    with pipeline.After(first, second):
      third = yield SaveRunOrder('third')
      fourth = yield SaveRunOrder('third')

      with pipeline.After(third, fourth):
        with pipeline.After(third):
          yield SaveRunOrder('fifth')

        with pipeline.After(fourth):
          yield SaveRunOrder('fifth')


class DoAfterList(pipeline.Pipeline):
  """Test the After clause with a list of jobs."""

  def run(self):
    job_list = []
    for i in xrange(10):
      job = yield EchoNamedHalfAsync(
          one='red', two='blue', three='green', four='yellow')
      job_list.append(job)

    with pipeline.After(*job_list):
      combined = yield common.Concat(*[job.one for job in job_list])
      result = yield SaveRunOrder(combined)
      with pipeline.After(result):
        yield SaveRunOrder('twelfth')


class DoInOrder(pipeline.Pipeline):
  """Test the InOrder clause."""

  def run(self):
    with pipeline.InOrder():
      yield SaveRunOrder('first')
      yield SaveRunOrder('second')
      yield SaveRunOrder('third')
      yield SaveRunOrder('fourth')


class DoInOrderNested(pipeline.Pipeline):
  """Test the InOrder clause when nested."""

  def run(self):
    with pipeline.InOrder():
      yield SaveRunOrder('first')
      yield SaveRunOrder('second')

      with pipeline.InOrder():
        # Should break.
        yield SaveRunOrder('third')
        yield SaveRunOrder('fourth')


class MixAfterInOrder(pipeline.Pipeline):
  """Test mixing After and InOrder clauses."""

  def run(self):
    first = yield SaveRunOrder('first')
    with pipeline.After(first):
      with pipeline.InOrder():
        yield SaveRunOrder('second')
        yield SaveRunOrder('third')
        fourth = yield SaveRunOrder('fourth')
    with pipeline.InOrder():
      with pipeline.After(fourth):
        yield SaveRunOrder('fifth')
        yield SaveRunOrder('sixth')


class RecordFinalized(pipeline.Pipeline):
  """Records calls to finalized."""

  def run(self, depth):
    yield SaveRunOrder('run#%d' % depth)

  def finalized(self):
    RunOrder.add('finalized#%d' % self.args[0])

  def finalized_test(self):
    RunOrder.add('finalized_test#%d' % self.args[0])


class NestedFinalize(pipeline.Pipeline):
  """Test nested pipelines are finalized in a reasonable order."""

  def run(self, depth):
    if depth == 0:
      return
    yield RecordFinalized(depth)
    yield NestedFinalize(depth - 1)


class YieldBadValue(pipeline.Pipeline):
  """Test pipeline that yields something that's not a pipeline."""

  def run(self):
    yield 5


class YieldChildTwice(pipeline.Pipeline):
  """Test pipeline that yields the same child pipeline twice."""

  def run(self):
    child = EchoSync('bad')
    yield child
    yield child


class FinalizeFailure(pipeline.Pipeline):
  """Test when finalized raises an error."""

  def run(self):
    pass

  def finalized(self):
    raise Exception('Doh something broke!')


class SyncForcesRetry(pipeline.Pipeline):
  """Test when a synchronous pipeline raises the Retry exception."""

  def run(self):
    raise pipeline.Retry('We need to try this again')


class AsyncForcesRetry(pipeline.Pipeline):
  """Test when a synchronous pipeline raises the Retry exception."""

  async = True

  def run(self):
    raise pipeline.Retry('We need to try this again')

  def run_test(self):
    raise pipeline.Retry('We need to try this again')


class GeneratorForcesRetry(pipeline.Pipeline):
  """Test when a generator pipeline raises the Retry exception."""

  def run(self):
    if False:
      yield 1
    raise pipeline.Retry('We need to try this again')


class SyncRaiseAbort(pipeline.Pipeline):
  """Raises an abort signal."""

  def run(self):
    RunOrder.add('run SyncRaiseAbort')
    raise pipeline.Abort('Gotta bail!')

  def finalized(self):
    RunOrder.add('finalized SyncRaiseAbort: %s' % self.was_aborted)


class AsyncRaiseAbort(pipeline.Pipeline):
  """Raises an abort signal in an asynchronous pipeline."""

  async = True

  def run(self):
    raise pipeline.Abort('Gotta bail!')

  def run_test(self):
    raise pipeline.Abort('Gotta bail!')


class GeneratorRaiseAbort(pipeline.Pipeline):
  """Raises an abort signal in a generator pipeline."""

  def run(self):
    if False:
      yield 1
    raise pipeline.Abort('Gotta bail!')


class AbortAndRecordFinalized(pipeline.Pipeline):
  """Records calls to finalized."""

  def run(self):
    RunOrder.add('run AbortAndRecordFinalized')
    yield SyncRaiseAbort()

  def finalized(self):
    RunOrder.add('finalized AbortAndRecordFinalized: %s' %
                 self.was_aborted)


class SetStatusPipeline(pipeline.Pipeline):
  """Simple pipeline that just sets its status a few times."""

  def run(self):
    self.set_status(message='My message')
    self.set_status(console_url='/path/to/my/console')
    self.set_status(status_links=dict(one='/red', two='/blue'))
    self.set_status(message='My message',
                    console_url='/path/to/my/console',
                    status_links=dict(one='/red', two='/blue'))


class PassBadValue(pipeline.Pipeline):
  """Simple pipeline that passes along a non-JSON serializable value."""

  def run(self):
    yield EchoSync(object())


class ReturnBadValue(pipeline.Pipeline):
  """Simple pipeline that returns a non-JSON serializable value."""

  def run(self):
    return object()


class EchoParams(pipeline.Pipeline):
  """Echos the parameters this pipeline has."""

  def run(self):
    ALLOWED = ('backoff_seconds', 'backoff_factor', 'max_attempts', 'target')
    return dict((key, getattr(self, key)) for key in ALLOWED)


class WithParams(pipeline.Pipeline):
  """Simple pipeline that uses the with_params helper method."""

  def run(self):
    foo = yield EchoParams().with_params(
        max_attempts=8,
        backoff_seconds=99,
        target='other-backend')
    yield EchoSync(foo, 'stuff')


class FunctionalTest(test_shared.TaskRunningMixin, TestBase):
  """End-to-end tests for various Pipeline constructs."""

  def setUp(self):
    """Sets up the test harness."""
    super(FunctionalTest, self).setUp()

  def testStartSync(self):
    """Tests starting and executing just a synchronous pipeline."""
    stage = EchoSync(1, 2, 3)
    self.assertFalse(stage.async)
    self.assertEquals((1, 2, 3), EchoSync(1, 2, 3).run(1, 2, 3))
    outputs = self.run_pipeline(stage)
    self.assertEquals([1, 2, 3], outputs.default.value)

  def testStartAsync(self):
    """Tests starting and executing an asynchronous pipeline."""
    stage = EchoAsync(1, 2, 3)
    self.assertTrue(stage.async)
    outputs = self.run_pipeline(stage)
    self.assertEquals([1, 2, 3], outputs.default.value)

  def testSyncNamedOutputs(self):
    """Tests a synchronous pipeline with named outputs."""
    stage = EchoParticularNamedSync(
        one='red', two='blue', three='green', four='yellow')
    self.assertFalse(stage.async)
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('red', outputs.one.value)
    self.assertEquals('blue', outputs.two.value)
    self.assertEquals('green', outputs.three.value)
    self.assertEquals('yellow', outputs.four.value)

  def testAsyncNamedOutputs(self):
    """Tests an asynchronous pipeline with named outputs."""
    stage = EchoParticularNamedAsync(
        one='red', two='blue', three='green', four='yellow')
    self.assertTrue(stage.async)
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('red', outputs.one.value)
    self.assertEquals('blue', outputs.two.value)
    self.assertEquals('green', outputs.three.value)
    self.assertEquals('yellow', outputs.four.value)

  def testInheirtOutputs(self):
    """Tests when a pipeline generator child inherits all parent outputs."""
    stage = FillAndPassParticular(
        [],
        one='red', two='blue', three='green', four='yellow',
        prefix='passed-')
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('passed-red', outputs.one.value)
    self.assertEquals('passed-blue', outputs.two.value)
    self.assertEquals('passed-green', outputs.three.value)
    self.assertEquals('passed-yellow', outputs.four.value)

  def testInheritOutputsPartial(self):
    """Tests when a pipeline generator child inherits some parent outputs."""
    stage = FillAndPassParticular(
        ['one', 'three'],
        one='red', two='blue', three='green', four='yellow',
        prefix='passed-')
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('red', outputs.one.value)
    self.assertEquals('passed-blue', outputs.two.value)
    self.assertEquals('green', outputs.three.value)
    self.assertEquals('passed-yellow', outputs.four.value)

  def testInheritOutputsStrict(self):
    """Tests strict child of a pipeline generator inherits all outputs."""
    stage = StrictChildInheritsAll(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('red', outputs.one.value)
    self.assertEquals('blue', outputs.two.value)
    self.assertEquals('green', outputs.three.value)
    self.assertEquals('yellow', outputs.four.value)

  def testInheritChildSyncStrictMissing(self):
    """Tests when a strict child pipeline does not output to a required slot."""
    stage = StrictChildInheritsAll(
        one='red', two='blue', three='green')
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testInheritChildSyncStrictNotDeclared(self):
    """Tests when a strict child pipeline outputs to an undeclared name."""
    stage = StrictChildInheritsAll(
        one='red', two='blue', three='green', four='yellow', five='undeclared')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testInheritGeneratorStrict(self):
    """Tests when a strict child pipeline inherits all outputs."""
    stage = StrictChildGeneratorInheritsAll(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('red', outputs.one.value)
    self.assertEquals('blue', outputs.two.value)
    self.assertEquals('green', outputs.three.value)
    self.assertEquals('yellow', outputs.four.value)

  def testInheritGeneratorStrictMissing(self):
    """Tests when a strict child generator does not output to a slot."""
    stage = StrictChildGeneratorInheritsAll(
        one='red', two='blue', three='green')
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testInheritGeneratorStrictNotDeclared(self):
    """Tests when a strict child generator outputs to an undeclared name."""
    stage = StrictChildGeneratorInheritsAll(
        one='red', two='blue', three='green', four='yellow', five='undeclared')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testPartialConsumptionStrict(self):
    """Tests when a parent pipeline consumes a subset of strict child outputs.

    When the child is strict, then partial consumption is fine since all
    outputs must be declared ahead of time.
    """
    stage = ConsumePartialChildrenStrict(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEquals(['red', 'blue'], outputs.default.value)

  def testPartialConsumptionDynamic(self):
    """Tests when a parent pipeline consumes a subset of dynamic child outputs.

    When the child is dynamic, then all outputs must be consumed by the caller.
    """
    stage = ConsumePartialChildren(
        one='red', two='blue', three='green', four='yellow')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testNoDefaultConsumption(self):
    """Tests when a parent pipeline does not consume default output."""
    stage = DoNotConsumeDefault('hi there')
    outputs = self.run_pipeline(stage)
    self.assertEquals('hi there', outputs.default.value)

  def testGeneratorNoChildren(self):
    """Tests when a generator pipeline yields no children."""
    self.assertRaises(StopIteration, FillAndPass([]).run([]).next)
    stage = FillAndPass([])
    outputs = self.run_pipeline(stage)
    self.assertTrue(outputs.default.value is None)

  def testSyncMissingNamedOutput(self):
    """Tests when a sync pipeline does not output to a named output."""
    stage = EchoParticularNamedSync(one='red', two='blue', three='green')
    self.assertFalse(stage.async)
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testGeneratorNoChildrenMissingNamedOutput(self):
    """Tests a missing output from a generator with no child pipelines."""
    stage = FillAndPassParticular(
        ['one', 'two', 'three'],
        one='red', two='blue', three='green')
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testSyncUndeclaredOutput(self):
    """Tests when a strict sync pipeline outputs to an undeclared output."""
    stage = EchoParticularNamedSync(
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertFalse(stage.async)
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testGeneratorChildlessUndeclaredOutput(self):
    """Tests when a childless generator outputs to an undeclared output."""
    stage = FillAndPassParticular(
        ['one', 'two', 'three', 'four', 'other'],
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testRootGeneratorChildInheritOutputUndeclared(self):
    """Tests when root's child inherits all and outputs to a bad name."""
    stage = FillAndPassParticular(
        ['one', 'two'],
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testDeepGeneratorChildInheritOutputUndeclared(self):
    """Tests when a pipeline that is not the root outputs to a bad name."""
    stage = TwoLevelFillAndPass(
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testDeepGenerator(self):
    """Tests a multi-level generator."""
    stage = TwoLevelFillAndPass(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('second-first-red', outputs.one.value)
    self.assertEquals('second-first-blue', outputs.two.value)
    self.assertEquals('second-green', outputs.three.value)
    self.assertEquals('second-yellow', outputs.four.value)

  def testDeepGenerator_Huge(self):
    """Tests a multi-level generator with huge inputs and outputs."""
    big_data = 'blue' * 1000000
    stage = TwoLevelFillAndPass(
        one='red', two=big_data, three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEquals(None, outputs.default.value)
    self.assertEquals('second-first-red', outputs.one.value)
    self.assertEquals('second-first-' + big_data, outputs.two.value)
    self.assertEquals('second-green', outputs.three.value)
    self.assertEquals('second-yellow', outputs.four.value)

  def testOnlyConsumePassedOnOutputs(self):
    """Tests that just accessing a Slot on a PipelineFuture won't consume it."""
    stage = UnusedOutputReference()
    outputs = self.run_pipeline(stage)
    self.assertEquals('yellow', outputs.default.value)

  def testAccessUndeclaredOutputsBreaks(self):
    """Tests errors accessing undeclared outputs on a default-only pipeline."""
    stage = AccessUndeclaredDefaultOnly()
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testGeneratorRecursive(self):
    """Tests a recursive nesting of generators."""
    stage = EuclidGCD(1071, 462)
    outputs = self.run_pipeline(stage)
    self.assertEquals(21, outputs.gcd.value)

    stage = EuclidGCD(1071, 463)
    outputs = self.run_pipeline(stage)
    self.assertEquals(1, outputs.gcd.value)

  def testAfter(self):
    """Tests the After() class."""
    stage = DoAfter()
    self.run_pipeline(stage)
    self.assertEquals(['first', 'first', 'third', 'third'],
                      RunOrder.get())

  def testAfterWithNesting(self):
    """Tests that After() nesting of the same dependencies doesn't break."""
    stage = DoAfterNested()
    self.run_pipeline(stage)
    self.assertEquals(['first', 'first', 'third', 'third', 'fifth', 'fifth'],
                      RunOrder.get())

  def testAfterWithList(self):
    """Tests that After() with a list of dependencies works."""
    stage = DoAfterList()
    self.run_pipeline(stage)
    self.assertEquals( ['redredredredredredredredredred', 'twelfth'],
                      RunOrder.get())

  def testInOrder(self):
    """Tests the InOrder() class."""
    stage = DoInOrder()
    self.run_pipeline(stage)
    self.assertEquals(['first', 'second', 'third', 'fourth'],
                      RunOrder.get())

  def testInOrderNesting(self):
    """Tests that InOrder nesting is not allowed."""
    stage = DoInOrderNested()
    self.assertRaises(
        pipeline.UnexpectedPipelineError, self.run_pipeline, stage)

  def testMixAfterInOrder(self):
    """Tests nesting Afters in InOrder blocks and vice versa."""
    stage = MixAfterInOrder()
    self.run_pipeline(stage)
    self.assertEquals(['first', 'second', 'third', 'fourth', 'fifth', 'sixth'],
                      RunOrder.get())

  def testFinalized(self):
    """Tests the order of finalization."""
    stage = NestedFinalize(5)
    self.run_pipeline(stage)
    run_order = RunOrder.get()

    # Ensure each entry is unique.
    self.assertEquals(10, len(set(run_order)))

    # That there are 5 run entries that are in reasonable order.
    run_entries = [
        int(r[len('run#'):]) for r in run_order
        if r.startswith('run#')]
    self.assertEquals(5, len(run_entries))
    self.assertEquals([5, 4, 3, 2, 1], run_entries)

    # That there are 5 finalized entries that are in reasonable order.
    if self.test_mode:
      finalized_name = 'finalized_test#'
    else:
      finalized_name = 'finalized#'

    finalized_entries = [
        int(r[len(finalized_name):]) for r in run_order
        if r.startswith(finalized_name)]
    self.assertEquals(5, len(finalized_entries))
    self.assertEquals([5, 4, 3, 2, 1], finalized_entries)

  def testRunTest(self):
    """Tests that run_test is preferred over run for test mode."""
    stage = RunMethod()
    outputs = self.run_pipeline(stage)
    if self.test_mode:
      self.assertEquals('run_test()', outputs.default.value)
    else:
      self.assertEquals('run()', outputs.default.value)

  def testYieldBadValue(self):
    """Tests yielding something that is invalid."""
    stage = YieldBadValue()
    self.assertRaises(
        pipeline.UnexpectedPipelineError, self.run_pipeline, stage)

  def testYieldPipelineInstanceTwice(self):
    """Tests when a Pipeline instance is yielded multiple times."""
    stage = YieldChildTwice()
    self.assertRaises(
        pipeline.UnexpectedPipelineError, self.run_pipeline, stage)

  def testFinalizeException(self):
    """Tests that finalized exceptions just raise up without being caught."""
    stage = FinalizeFailure()
    try:
      self.run_pipeline(stage)
      self.fail('Should have raised')
    except Exception, e:
      self.assertEquals('Doh something broke!', str(e))

  def testSyncRetryException(self):
    """Tests when a sync generator raises a Retry exception."""
    stage = SyncForcesRetry()
    self.assertRaises(pipeline.Retry, self.run_pipeline, stage)

  def testAsyncRetryException(self):
    """Tests when an async generator raises a Retry exception."""
    stage = AsyncForcesRetry()
    self.assertRaises(pipeline.Retry, self.run_pipeline, stage)

  def testGeneratorRetryException(self):
    """Tests when a generator raises a Retry exception."""
    stage = GeneratorForcesRetry()
    self.assertRaises(pipeline.Retry, self.run_pipeline, stage)

  def testSyncAbortException(self):
    """Tests when a sync pipeline raises an abort exception."""
    stage = SyncRaiseAbort()
    self.assertRaises(pipeline.Abort, self.run_pipeline, stage)

  def testAsyncAbortException(self):
    """Tests when an async pipeline raises an abort exception."""
    stage = AsyncRaiseAbort()
    self.assertRaises(pipeline.Abort, self.run_pipeline, stage)

  def testGeneratorAbortException(self):
    """Tests when a generator pipeline raises an abort exception."""
    stage = GeneratorRaiseAbort()
    self.assertRaises(pipeline.Abort, self.run_pipeline, stage)

  def testAbortThenFinalize(self):
    """Tests that pipelines are finalized after abort is raised.

    This test requires special handling for different modes to confirm that
    finalization happens after abort in production mode.
    """
    stage = AbortAndRecordFinalized()
    if self.test_mode:
      # Finalize after abort doesn't happen in test mode.
      try:
        self.run_pipeline(stage)
        self.fail('Should have raised')
      except Exception, e:
        self.assertEquals('Gotta bail!', str(e))

      run_order = RunOrder.get()
      self.assertEquals(['run AbortAndRecordFinalized', 'run SyncRaiseAbort'],
                        run_order)
    else:
      self.run_pipeline(stage, _task_retry=False, _require_slots_filled=False)
      # Non-deterministic results for finalize. Must equal one of these two.
      expected_order1 = [
          'run AbortAndRecordFinalized',
          'run SyncRaiseAbort',
          'finalized SyncRaiseAbort: True',
          'finalized AbortAndRecordFinalized: True',
      ]
      expected_order2 = [
          'run AbortAndRecordFinalized',
          'run SyncRaiseAbort',
          'finalized AbortAndRecordFinalized: True',
          'finalized SyncRaiseAbort: True',
      ]
      run_order = RunOrder.get()
      self.assertTrue(run_order == expected_order1 or
                      run_order == expected_order2,
                      'Found order: %r' % run_order)

  def testSetStatus_Working(self):
    """Tests that setting the status does not raise errors."""
    stage = SetStatusPipeline()
    self.run_pipeline(stage)
    # That's it. No exceptions raised.

  def testPassBadValue(self):
    """Tests when a pipeline passes a non-serializable value to a child."""
    stage = PassBadValue()
    if self.test_mode:
      self.assertRaises(TypeError, self.run_pipeline, stage)
    else:
      self.assertRaises(
          TypeError, self.run_pipeline,
          stage, _task_retry=False, _require_slots_filled=False)

  def testReturnBadValue(self):
    """Tests when a pipeline returns a non-serializable value."""
    stage = ReturnBadValue()
    if self.test_mode:
      self.assertRaises(TypeError, self.run_pipeline, stage)
    else:
      self.assertRaises(
          TypeError, self.run_pipeline,
          stage, _task_retry=False, _require_slots_filled=False)

  def testWithParams(self):
    """Tests when a pipeline uses the with_params helper."""
    stage = WithParams()
    outputs = self.run_pipeline(stage)
    if self.test_mode:
      # In test mode you cannot modify the runtime parameters.
      self.assertEquals(
          [
            {
              'backoff_seconds': 15,
              'backoff_factor': 2,
              'target': None,
              'max_attempts': 3
            },
            'stuff'
          ],
          outputs.default.value)
    else:
      self.assertEquals(
          [
            {
              'backoff_seconds': 99,
              'backoff_factor': 2,
              'target': 'other-backend',
              'max_attempts': 8
            },
            'stuff',
          ],
          outputs.default.value)


class FunctionalTestModeTest(test_shared.TestModeMixin, FunctionalTest):
  """Runs all functional tests in test mode."""

  DO_NOT_DELETE = 'Seriously... We only need the class declaration.'


class StatusTest(TestBase):
  """Tests for the status handlers."""

  def setUp(self):
    """Sets up the test harness."""
    TestBase.setUp(self)

    self.fill_time = datetime.datetime(2010, 12, 10, 13, 55, 16, 416567)

    self.pipeline1_key = db.Key.from_path(_PipelineRecord.kind(), 'one')
    self.pipeline2_key = db.Key.from_path(_PipelineRecord.kind(), 'two')
    self.pipeline3_key = db.Key.from_path(_PipelineRecord.kind(), 'three')
    self.slot1_key = db.Key.from_path(_SlotRecord.kind(), 'red')
    self.slot2_key = db.Key.from_path(_SlotRecord.kind(), 'blue')
    self.slot3_key = db.Key.from_path(_SlotRecord.kind(), 'green')

    self.slot1_record = _SlotRecord(
        key=self.slot1_key,
        root_pipeline=self.pipeline1_key)
    self.slot2_record = _SlotRecord(
        key=self.slot2_key,
        root_pipeline=self.pipeline1_key)
    self.slot3_record = _SlotRecord(
        key=self.slot3_key,
        root_pipeline=self.pipeline1_key)

    self.base_params = {
       'args': [],
       'kwargs': {},
       'task_retry': False,
       'backoff_seconds': 1,
       'backoff_factor': 2,
       'max_attempts': 4,
       'queue_name': 'default',
       'base_path': '',
       'after_all': [],
    }
    self.params1 = self.base_params.copy()
    self.params1.update({
       'output_slots': {'default': str(self.slot1_key)},
    })
    self.params2 = self.base_params.copy()
    self.params2.update({
       'output_slots': {'default': str(self.slot2_key)},
    })
    self.params3 = self.base_params.copy()
    self.params3.update({
       'output_slots': {'default': str(self.slot3_key)},
    })

    self.pipeline1_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.RUN,
        class_path='does.not.exist1',
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(self.params1),
        key=self.pipeline1_key,
        max_attempts=4)
    self.pipeline2_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.WAITING,
        class_path='does.not.exist2',
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(self.params2),
        key=self.pipeline2_key,
        max_attempts=3)
    self.pipeline3_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.DONE,
        class_path='does.not.exist3',
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(self.params3),
        key=self.pipeline3_key,
        max_attempts=2)

    self.barrier1_record = _BarrierRecord(
        parent=self.pipeline1_key,
        key_name=_BarrierRecord.FINALIZE,
        target=self.pipeline1_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[self.slot1_key])
    self.barrier2_record = _BarrierRecord(
        parent=self.pipeline2_key,
        key_name=_BarrierRecord.FINALIZE,
        target=self.pipeline2_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[self.slot2_key])
    self.barrier2_record_start = _BarrierRecord(
        parent=self.pipeline2_key,
        key_name=_BarrierRecord.START,
        target=self.pipeline2_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[])
    self.barrier3_record = _BarrierRecord(
        parent=self.pipeline3_key,
        key_name=_BarrierRecord.FINALIZE,
        target=self.pipeline3_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[self.slot3_key])

  def testGetTimestampMs(self):
    """Tests for the _get_timestamp_ms function."""
    when = datetime.datetime(2010, 12, 10, 13, 55, 16, 416567)
    self.assertEquals(1291989316416L, pipeline._get_timestamp_ms(when))

  def testGetInternalStatus_Missing(self):
    """Tests for _get_internal_status when the pipeline is missing."""
    try:
      pipeline._get_internal_status(pipeline_key=self.pipeline1_key)
      self.fail('Did not raise')
    except pipeline.PipelineStatusError, e:
      self.assertEquals('Could not find pipeline ID "one"', str(e))

  def testGetInternalStatus_OutputSlotMissing(self):
    """Tests for _get_internal_status when the output slot is missing."""
    try:
      pipeline._get_internal_status(
          pipeline_key=self.pipeline1_key,
          pipeline_dict={self.pipeline1_key: self.pipeline1_record},
          barrier_dict={self.barrier1_record.key(): self.barrier1_record})
      self.fail('Did not raise')
    except pipeline.PipelineStatusError, e:
      self.assertEquals(
          'Default output slot with '
          'key=aglteS1hcHAtaWRyGgsSEV9BRV9QaXBlbGluZV9TbG90IgNyZWQM '
          'missing for pipeline ID "one"', str(e))

  def testGetInternalStatus_FinalizeBarrierMissing(self):
    """Tests for _get_internal_status when the finalize barrier is missing."""
    try:
      pipeline._get_internal_status(
          pipeline_key=self.pipeline1_key,
          pipeline_dict={self.pipeline1_key: self.pipeline1_record},
          slot_dict={self.slot1_key: self.slot1_record})
      self.fail('Did not raise')
    except pipeline.PipelineStatusError, e:
      self.assertEquals(
          'Finalization barrier missing for pipeline ID "one"', str(e))

  def testGetInternalStatus_Finalizing(self):
    """Tests for _get_internal_status when the status is finalizing."""
    self.slot1_record.status = _SlotRecord.FILLED
    self.slot1_record.fill_time = self.fill_time

    expected = {
      'status': 'finalizing',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'outputs': {
        'default': str(self.slot1_key),
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'endTimeMs': 1291989316416L,
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key(): self.barrier1_record}))

  def testGetInternalStatus_Retry(self):
    """Tests for _get_internal_status when the status is retry."""
    self.pipeline2_record.next_retry_time = self.fill_time
    self.pipeline2_record.retry_message = 'My retry message'

    expected = {
      'status': 'retry',
      'lastRetryMessage': 'My retry message',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'startTimeMs': 1291989316416L,
      'outputs': {
        'default': str(self.slot2_key),
      },
      'args': [],
      'classPath': 'does.not.exist2',
      'children': [],
      'maxAttempts': 3,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline2_key,
        pipeline_dict={self.pipeline2_key: self.pipeline2_record},
        slot_dict={self.slot2_key: self.slot1_record},
        barrier_dict={self.barrier2_record.key(): self.barrier2_record}))

  def testGetInternalStatus_Waiting(self):
    """Tests for _get_internal_status when the status is waiting."""
    expected = {
      'status': 'waiting',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'outputs': {
        'default': str(self.slot2_key)
      },
      'args': [],
      'classPath': 'does.not.exist2',
      'children': [],
      'maxAttempts': 3,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline2_key,
        pipeline_dict={self.pipeline2_key: self.pipeline2_record},
        slot_dict={self.slot2_key: self.slot1_record},
        barrier_dict={
            self.barrier2_record.key(): self.barrier2_record,
            self.barrier2_record_start.key(): self.barrier2_record_start}))

  def testGetInternalStatus_Run(self):
    """Tests for _get_internal_status when the status is run."""
    self.pipeline1_record.start_time = self.fill_time

    expected = {
      'status': 'run',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'startTimeMs': 1291989316416L,
      'outputs': {
        'default': str(self.slot1_key)
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key(): self.barrier1_record}))

  def testGetInternalStatus_RunAfterRetry(self):
    """Tests _get_internal_status when a stage is re-run on retrying."""
    self.pipeline1_record.start_time = self.fill_time
    self.pipeline1_record.next_retry_time = self.fill_time
    self.pipeline1_record.retry_message = 'My retry message'
    self.pipeline1_record.current_attempt = 1

    expected = {
      'status': 'run',
      'currentAttempt': 2,
      'lastRetryMessage': 'My retry message',
      'afterSlotKeys': [],
      'startTimeMs': 1291989316416L,
      'outputs': {
        'default': str(self.slot1_key)
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key(): self.barrier1_record}))

  def testGetInternalStatus_Aborted(self):
    """Tests for _get_internal_status when the status is aborted."""
    self.pipeline1_record.status = _PipelineRecord.ABORTED
    self.pipeline1_record.abort_message = 'I had to bail'

    expected = {
      'status': 'aborted',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'abortMessage': 'I had to bail',
      'outputs': {
        'default': str(self.slot1_key),
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key(): self.barrier1_record}))

  def testGetInternalStatus_MoreParams(self):
    """Tests for _get_internal_status with children, slots, and outputs."""
    self.pipeline1_record.start_time = self.fill_time
    self.pipeline1_record.fanned_out = [
        self.pipeline2_key, self.pipeline3_key]
    self.pipeline1_record.params['args'] = [
        {'type': 'slot', 'slot_key': 'foobar'},
        {'type': 'slot', 'slot_key': 'meepa'},
    ]
    self.pipeline1_record.params['kwargs'] = {
        'my_arg': {'type': 'slot', 'slot_key': 'other'},
        'second_arg': {'type': 'value', 'value': 1234},
    }
    self.pipeline1_record.params['output_slots'] = {
      'default': str(self.slot1_key),
      'another_one': str(self.slot2_key),
    }
    self.pipeline1_record.params['after_all'] = [
      str(self.slot2_key),
    ]

    expected = {
        'status': 'run',
        'currentAttempt': 1,
        'afterSlotKeys': [
          'aglteS1hcHAtaWRyGwsSEV9BRV9QaXBlbGluZV9TbG90IgRibHVlDA'
        ],
        'startTimeMs': 1291989316416L,
        'outputs': {
          'default': 'aglteS1hcHAtaWRyGgsSEV9BRV9QaXBlbGluZV9TbG90IgNyZWQM',
          'another_one':
              'aglteS1hcHAtaWRyGwsSEV9BRV9QaXBlbGluZV9TbG90IgRibHVlDA',
        },
        'args': [
          {'type': 'slot', 'slotKey': 'foobar'},
          {'type': 'slot', 'slotKey': 'meepa'}
        ],
        'classPath': 'does.not.exist1',
        'children': [u'two', u'three'],
        'maxAttempts': 4,
        'kwargs': {
          'my_arg': {'type': 'slot', 'slotKey': 'other'},
          'second_arg': {'type': 'value', 'value': 1234},
        },
        'backoffFactor': 2,
        'backoffSeconds': 1,
        'queueName': 'default'
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key(): self.barrier1_record}))

  def testGetInternalStatus_StatusRecord(self):
    """Tests for _get_internal_status when the status record is present."""
    status_record = _StatusRecord(
        key=db.Key.from_path(_StatusRecord.kind(), self.pipeline1_key.name()),
        message='My status message',
        status_time=self.fill_time,
        console_url='/path/to/console',
        link_names=[db.Text(x) for x in ('one', 'two', 'three')],
        link_urls=[db.Text(x) for x in ('/first', '/second', '/third')],
        root_pipeline=self.pipeline1_key)

    expected = {
        'status': 'run',
        'currentAttempt': 1,
        'afterSlotKeys': [],
        'statusTimeMs': 1291989316416L,
        'outputs': {
          'default': str(self.slot1_key)
        },
        'args': [],
        'classPath': 'does.not.exist1',
        'children': [],
        'maxAttempts': 4,
        'kwargs': {},
        'backoffFactor': 2,
        'backoffSeconds': 1,
        'queueName': 'default',
        'statusLinks': {
          'three': '/third',
          'two': '/second',
          'one': '/first'
        },
        'statusConsoleUrl': '/path/to/console',
        'statusMessage': 'My status message',
    }

    self.assertEquals(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key(): self.barrier1_record},
        status_dict={status_record.key(): status_record}))

  def testGetInternalSlot_Missing(self):
    """Tests _get_internal_slot when the slot is missing."""
    try:
      pipeline._get_internal_slot(slot_key=self.slot1_key)
      self.fail('Did not raise')
    except pipeline.PipelineStatusError, e:
      self.assertEquals(
          'Could not find data for output slot key '
          '"aglteS1hcHAtaWRyGgsSEV9BRV9QaXBlbGluZV9TbG90IgNyZWQM".',
          str(e))

  def testGetInternalSlot_Filled(self):
    """Tests _get_internal_slot when the slot is filled."""
    self.slot1_record.status = _SlotRecord.FILLED
    self.slot1_record.filler = self.pipeline2_key
    self.slot1_record.fill_time = self.fill_time
    self.slot1_record.root_pipeline = self.pipeline1_key
    self.slot1_record.value_text = simplejson.dumps({
        'one': 1234, 'two': 'hello'})
    expected = {
        'status': 'filled',
        'fillerPipelineId': 'two',
        'value': {'two': 'hello', 'one': 1234},
        'fillTimeMs': 1291989316416L
    }
    self.assertEquals(
        expected,
        pipeline._get_internal_slot(
            slot_key=self.slot1_key,
            slot_dict={self.slot1_key: self.slot1_record}))

  def testGetInternalSlot_Waiting(self):
    """Tests _get_internal_slot when the slot is waiting."""
    self.slot1_record.status = _SlotRecord.WAITING
    self.slot1_record.root_pipeline = self.pipeline1_key
    expected = {
        'status': 'waiting',
        'fillerPipelineId': 'two',
    }
    self.assertEquals(
        expected,
        pipeline._get_internal_slot(
            slot_key=self.slot1_key,
            slot_dict={self.slot1_key: self.slot1_record},
            filler_pipeline_key=self.pipeline2_key))

  def testGetStatusTree_RootMissing(self):
    """Tests get_status_tree when the root pipeline is missing."""
    try:
      pipeline.get_status_tree(self.pipeline1_key.name())
      self.fail('Did not raise')
    except pipeline.PipelineStatusError, e:
      self.assertEquals('Could not find pipeline ID "one"', str(e))

  def testGetStatusTree_NotRoot(self):
    """Tests get_status_tree when the pipeline query is not the root."""
    self.pipeline1_record.root_pipeline = self.pipeline2_key
    db.put([self.pipeline1_record])

    try:
      pipeline.get_status_tree(self.pipeline1_key.name())
      self.fail('Did not raise')
    except pipeline.PipelineStatusError, e:
      self.assertEquals('Pipeline ID "one" is not a root pipeline!', str(e))

  def testGetStatusTree_ChildMissing(self):
    """Tests get_status_tree when a fanned out child pipeline is missing."""
    self.pipeline1_record.fanned_out = [self.pipeline2_key]
    db.put([self.pipeline1_record, self.barrier1_record, self.slot1_record])

    try:
      pipeline.get_status_tree(self.pipeline1_key.name())
      self.fail('Did not raise')
    except pipeline.PipelineStatusError, e:
      self.assertEquals(
          'Pipeline ID "one" points to child ID "two" which does not exist.',
          str(e))

  def testGetStatusTree_Example(self):
    """Tests a full example of a good get_status_tree response."""
    self.pipeline1_record.fanned_out = [self.pipeline2_key, self.pipeline3_key]
    self.slot1_record.root_pipeline = self.pipeline1_key
    self.pipeline3_record.finalized_time = self.fill_time

    # This one looks like a child, but it will be ignored since it is not
    # reachable from the root via the fanned_out property.
    bad_pipeline_key = db.Key.from_path(_PipelineRecord.kind(), 'ignored')
    bad_pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.RUN,
        class_path='does.not.exist4',
        # Bug in DB means we need to use the storage name here,
        # not the local property name.
        params=simplejson.dumps(self.params1),
        key=bad_pipeline_key,
        max_attempts=4)

    db.put([
        self.pipeline1_record, self.pipeline2_record, self.pipeline3_record,
        self.barrier1_record, self.barrier2_record, self.barrier3_record,
        self.slot1_record, self.slot2_record, self.slot3_record,
        bad_pipeline_record])

    expected = {
        'rootPipelineId': 'one',
        'pipelines': {
            'three': {
              'status': 'done',
              'currentAttempt': 1L,
              'afterSlotKeys': [],
              'outputs': {
                  'default': str(self.slot3_key)
              },
              'args': [],
              'classPath': 'does.not.exist3',
              'children': [],
              'endTimeMs': 1291989316416L,
              'maxAttempts': 2L,
              'kwargs': {},
              'backoffFactor': 2,
              'backoffSeconds': 1,
              'queueName': 'default'
            },
            'two': {
                'status': 'run',
                'currentAttempt': 1L,
                'afterSlotKeys': [],
                'outputs': {
                    'default': str(self.slot2_key)
                },
                'args': [],
                'classPath': 'does.not.exist2',
                'children': [],
                'maxAttempts': 3L,
                'kwargs': {},
                'backoffFactor': 2,
                'backoffSeconds': 1,
                'queueName': 'default'
            },
            'one': {
                'status': 'run',
                'currentAttempt': 1L,
                'afterSlotKeys': [],
                'outputs': {
                    'default': str(self.slot1_key)
                },
                'args': [],
                'classPath': 'does.not.exist1',
                'children': ['two', 'three'],
                'maxAttempts': 4L,
                'kwargs': {},
                'backoffFactor': 2,
                'backoffSeconds': 1,
                'queueName': 'default'
            }
        },
        'slots': {
            str(self.slot2_key): {
                'status': 'waiting',
                'fillerPipelineId': 'two'
            },
            str(self.slot3_key): {
                'status': 'waiting',
                'fillerPipelineId': 'three'
            }
        }
    }

    self.assertEquals(
        expected,
        pipeline.get_status_tree(self.pipeline1_key.name()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
