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

"""Demo Pipeline API application."""

from __future__ import with_statement

import logging
import os
import time

from google.appengine.api import mail
from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import util

import pipeline
from pipeline import common


################################################################################
# An example pipeline for generating reports.

class LongCount(pipeline.Pipeline):

  def run(self, entity_kind, property_name, value):
    cursor = None
    count = 0
    while True:
      query = db.GqlQuery(
          'SELECT * FROM %s WHERE %s = :1' % (entity_kind, property_name),
          value.lower(), key_only=True, cursor=cursor)
      result = query.fetch(1000)
      count += len(result)
      if len(result) < 1000:
        return (entity_kind, property_name, value, count)
      else:
        cursor = query.cursor()


class SplitCount(pipeline.Pipeline):

  def run(self, entity_kind, property_name, *value_list):
    all_counts = []
    for value in value_list:
      stage = yield LongCount(entity_kind, property_name, value)
      all_counts.append(stage)

    yield common.Append(*all_counts)


class UselessPipeline(pipeline.Pipeline):
  """This pipeline is totally useless. It just demostrates that it will run
  in parallel, yield a named output, and properly be ignored by the system.
  """

  def run(self):
    if not self.test_mode and self.current_attempt == 1:
      self.set_status(message='Pretending to fail, will retry shortly.',
                      console_url='/static/console.html',
                      status_links={'Home': '/'})
      raise pipeline.Retry('Whoops, I need to retry')

    # Looks like a generator, but it's not.
    if False:
      yield common.Log.info('Okay!')

    self.fill('coolness', 1234)


class EmailCountReport(pipeline.Pipeline):

  def run(self, receiver_email_address, kind_count_list):
    body = [
        'At %s the counts are:' % time.ctime()
    ]
    result_sum = 0
    for (entity_kind, property_name, value, count) in kind_count_list:
      body.append('%s.%s = "%s" -> %d' % (
                  entity_kind, property_name, value, count))
      result_sum += count

    rendered = '\n'.join(body)
    logging.info('Email body is:\n%s', rendered)

    # Works in production, I swear!
    sender = '%s@%s.appspotmail.com' % (os.environ['APPLICATION_ID'],
                                        os.environ['APPLICATION_ID'])
    try:
      mail.send_mail(
          sender=sender,
          to=receiver_email_address,
          subject='Entity count report',
          body=rendered)
    except (mail.InvalidSenderError, mail.InvalidEmailError):
      logging.exception('This should work in production.')

    return result_sum


class CountReport(pipeline.Pipeline):

  def run(self, email_address, entity_kind, property_name, *value_list):
    yield common.Log.info('UselessPipeline.coolness = %s',
                          (yield UselessPipeline()).coolness)

    split_counts = yield SplitCount(entity_kind, property_name, *value_list)
    yield common.Log.info('SplitCount result = %s', split_counts)

    with pipeline.After(split_counts):
      with pipeline.InOrder():
        yield common.Delay(seconds=1)
        yield common.Log.info('Done waiting')
        yield EmailCountReport(email_address, split_counts)

  def finalized(self):
    if not self.was_aborted:
      logging.info('All done! Found %s results', self.outputs.default.value)

################################################################################
# Silly guestbook application to run the pipelines on.

class GuestbookPost(db.Model):
  color = db.StringProperty()
  write_time = db.DateTimeProperty(auto_now_add=True)


class StartPipelineHandler(webapp.RequestHandler):
  def get(self):
    self.response.out.write(template.render('start.html', {}))

  def post(self):
    colors = [color for color in self.request.get_all('color') if color]
    job = CountReport(
        users.get_current_user().email(),
        GuestbookPost.kind(),
        'color',
        *colors)
    job.start()
    self.redirect('/_ah/pipeline/status?root=%s' % job.pipeline_id)


class MainHandler(webapp.RequestHandler):
  def get(self):
    context = {'posts': GuestbookPost.all().order('-write_time').fetch(100)}
    self.response.out.write(template.render('guestbook.html', context))

  def post(self):
    color = self.request.get('color')
    if color:
      GuestbookPost(color=color.lower()).put()
    self.redirect('/')


def main():
  application = webapp.WSGIApplication([
      (r'/', MainHandler),
      (r'/pipeline', StartPipelineHandler),
  ], debug=True)
  util.run_wsgi_app(application)


if __name__ == '__main__':
    main()
