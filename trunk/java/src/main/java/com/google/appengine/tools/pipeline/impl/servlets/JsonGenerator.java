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

package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.model.SlotDescriptor;
import com.google.appengine.tools.pipeline.impl.util.JsonUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for generating the Json that is consumed by the
 * JavaScript file status.js.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class JsonGenerator {

  private final PipelineObjects pipelineObjects;
  private String jsonRepresentation;

  public JsonGenerator(PipelineObjects pipelineObjects) {
    this.pipelineObjects = pipelineObjects;
  }

  public synchronized String getJson() {
    if (null == jsonRepresentation) {
      buildJsonRepresentation();
    }
    return jsonRepresentation;
  }

  private static String toString(Key key) {
    return KeyFactory.keyToString(key);
  }

  private static final String ROOT_PIPELINE_ID = "rootPipelineId";
  private static final String SLOTS = "slots";
  private static final String PIPELINES = "pipelines";
  private static final String SLOT_STATUS = "status";
  private static final String FILLED_STATUS = "filled";
  private static final String WAITING_STATUS = "waiting";
  private static final String SLOT_VALUE = "value";
  private static final String SLOT_FILL_TIME = "fillTimeMs";
  private static final String SLOT_SOURCE_JOB = "fillerPipelineId";
  private static final String JOB_CLASS = "classPath";
  private static final String JOB_STATUS = "status";
  private static final String RUN_STATUS = "run";
  private static final String DONE_STATUS = "done";
  private static final String ABORTED_STATUS = "aborted";
  private static final String RETRY_STATUS = "retry";
  private static final String CANCELED_STATUS = "canceled";

  // private static final String FINALIZING_STATUS = "finalizing";
  private static final String JOB_START_TIME = "startTimeMs";
  private static final String JOB_END_TIME = "endTimeMs";
  private static final String JOB_CHILDREN = "children";
  // List of positional argument slot dictionaries.
  private static final String JOB_ARGS = "args";
  // Dictionary of output slot dictionaries.
  private static final String JOB_OUTPUTS = "outputs";
  // Queue on which this pipeline is running.
  private static final String JOB_QUEUE_NAME = "queueName";
  // List of Slot Ids after which this pipeline runs.
  private static final String JOB_AFTER_SLOT_KEYS = "afterSlotKeys";
  // Number of the current attempt, starting at 1.
  private static final String JOB_CURRENT_ATTEMPT = "currentAttempt";
  // Maximum number of attempts before aborting
  private static final String JOB_MAX_ATTEMPTS = "maxAttempts";
  // Constant factor for backoff before retrying
  private static final String JOB_BACKOFF_SECONDS = "backoffSeconds";
  // Exponential factor for backoff before retrying
  private static final String JOB_BACKOFF_FACTOR = "backoffFactor";
  // Dictionary of keyword argument slot dictionaries.
  private static final String JOB_KWARGS = "kwargs";
  // Why the pipeline failed during the last retry, if there was a failure; may be empty
  private static final String JOB_LAST_RETRY_MESSAGE = "lastRetryMessage";
  // For root pipelines, why the pipeline was aborted if it was aborted; may be empty
  private static final String JOB_ABORT_MESSAGE = "abortMessage"; // For root
  private static final String DEFAULT_OUTPUT_NAME = "default";
  private static final String JOB_STATUS_CONSOLE_URL = "statusConsoleUrl";

  private void buildJsonRepresentation() {
    Map<String, Object> mapRepresentation = toMapRepresentation();
    jsonRepresentation = JsonUtils.mapToJson(mapRepresentation);
  }

  private Map<String, Object> toMapRepresentation() {
    Map<String, Map<String, Object>> slotMap = new HashMap<>(pipelineObjects.slots.size());
    Map<String, Map<String, Object>> jobMap = new HashMap<>(pipelineObjects.jobs.size());
    Map<String, Object> topLevel = new HashMap<>(4);
    topLevel.put(ROOT_PIPELINE_ID, pipelineObjects.rootJob.getKey().getName());
    topLevel.put(SLOTS, slotMap);
    topLevel.put(PIPELINES, jobMap);
    for (Slot slot : pipelineObjects.slots.values()) {
      slotMap.put(toString(slot.getKey()), buildMapRepresentation(slot));
    }
    for (JobRecord jobRecord : pipelineObjects.jobs.values()) {
      jobMap.put(jobRecord.getKey().getName(), buildMapRepresentation(jobRecord));
    }
    return topLevel;
  }

  private Map<String, Object> buildMapRepresentation(Slot slot) {
    Map<String, Object> map = new HashMap<>(5);
    String statusString = (slot.isFilled() ? FILLED_STATUS : WAITING_STATUS);
    map.put(SLOT_STATUS, statusString);
    map.put(SLOT_VALUE, slot.getValue());
    Date fillTime = slot.getFillTime();
    if (null != fillTime) {
      map.put(SLOT_FILL_TIME, fillTime.getTime());
    }
    Key sourceJobKey = slot.getSourceJobKey();
    if (null != sourceJobKey) {
      map.put(SLOT_SOURCE_JOB, sourceJobKey.getName());
    }
    return map;
  }

  private Map<String, Object> buildMapRepresentation(JobRecord jobRecord) {
    Map<String, Object> map = new HashMap<>(5);
    map.put(JOB_CLASS, jobRecord.getJobInstanceInflated().getJobClass());
    String statusString = null;
    switch (jobRecord.getState()) {
      case WAITING_TO_RUN:
        statusString = WAITING_STATUS;
        break;
      case WAITING_TO_FINALIZE:
        statusString = RUN_STATUS;
        break;
      case FINALIZED:
        statusString = DONE_STATUS;
        break;
      case STOPPED:
        statusString = ABORTED_STATUS;
        break;
      case RETRY:
        statusString = RETRY_STATUS;
        break;
      case CANCELED:
        statusString = CANCELED_STATUS;
        break;
      default:
        break;
    }
    map.put(JOB_STATUS, statusString);
    Date startTime = jobRecord.getStartTime();
    if (null != startTime) {
      map.put(JOB_START_TIME, startTime.getTime());
    }
    Date endTime = jobRecord.getEndTime();
    if (null != endTime) {
      map.put(JOB_END_TIME, endTime.getTime());
    }
    map.put(JOB_CHILDREN, buildArrayRepresentation(jobRecord.getChildKeys()));
    List<Map<String, Object>> argumentListRepresentation = new LinkedList<>();
    List<String> waitingOnRepresentation = new LinkedList<>();
    populateJobArgumentRepresentation(argumentListRepresentation, waitingOnRepresentation,
        jobRecord.getRunBarrierInflated().getWaitingOnInflated());
    map.put(JOB_ARGS, argumentListRepresentation);
    map.put(JOB_AFTER_SLOT_KEYS, waitingOnRepresentation);
    Map<String, String> allOutputs = new HashMap<>();
    String outputSlotId = toString(jobRecord.getOutputSlotKey());
    // Python Pipeline has the notion of multiple outputs with a distinguished
    // output named "default". We don't have that notion. We have only
    // one output and so we put it in "default".
    allOutputs.put(DEFAULT_OUTPUT_NAME, outputSlotId);
    map.put(JOB_OUTPUTS, allOutputs);
    map.put(JOB_QUEUE_NAME, "");
    map.put(JOB_CURRENT_ATTEMPT, jobRecord.getAttemptNumber());
    map.put(JOB_MAX_ATTEMPTS, jobRecord.getMaxAttempts());
    map.put(JOB_BACKOFF_SECONDS, jobRecord.getBackoffSeconds());
    map.put(JOB_BACKOFF_FACTOR, jobRecord.getBackoffFactor());
    map.put(JOB_KWARGS, new HashMap<String, String>());
    map.put(JOB_LAST_RETRY_MESSAGE, "");
    map.put(JOB_ABORT_MESSAGE, "");
    if (jobRecord.getStatusConsoleUrl() != null) {
      map.put(JOB_STATUS_CONSOLE_URL, jobRecord.getStatusConsoleUrl());
    }
    return map;
  }

  private void populateJobArgumentRepresentation(
      List<Map<String, Object>> argumentListRepresentation, List<String> waitingOnRepresentation,
      List<SlotDescriptor> slotDescriptors) {
    for (SlotDescriptor slotDescriptor : slotDescriptors) {
      Slot slot = slotDescriptor.slot;
      String slotId = toString(slot.getKey());
      if (slotDescriptor.isPhantom()) {
        waitingOnRepresentation.add(slotId);
      } else {
        argumentListRepresentation.add(buildArgumentRepresentation(slotDescriptor.slot));
      }
    }
  }

  private Map<String, Object> buildArgumentRepresentation(Slot slot) {
    Map<String, Object> map = new HashMap<>(3);
    if (slot.isFilled()) {
      map.put("type", "value");
      map.put("value", slot.getValue());
    } else {
      map.put("type", "slot");
      map.put("slot_key", toString(slot.getKey()));
    }

    return map;
  }

  private String[] buildArrayRepresentation(List<Key> listOfKeys) {
    String[] arrayOfIds = new String[listOfKeys.size()];
    int i = 0;
    for (Key key : listOfKeys) {
      arrayOfIds[i++] = key.getName();
    }
    return arrayOfIds;
  }
}
