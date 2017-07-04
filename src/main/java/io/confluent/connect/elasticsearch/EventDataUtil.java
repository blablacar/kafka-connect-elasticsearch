/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EventDataUtil {
  private static final Logger log = LoggerFactory.getLogger(EventDataUtil.class);

  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
  private static final ObjectMapper objectMapper = new ObjectMapper();

  protected static ObjectNode sinkRecordToJsonNode(SinkRecord sinkRecord) {
    try {
      String json = objectMapper.writeValueAsString(sinkRecord.value());
      return objectMapper.readValue(json, ObjectNode.class);
    } catch (IOException e) {
      log.error("failed to read json from sink record: " + sinkRecord.value());
    }

    return null;
  }

  protected static String toDateIndex(
      boolean enabled,
      String index,
      ObjectNode node,
      String fieldName) {

    if (!enabled) {
      return index;
    }

    return index + "-" + fetchFormattedEventDate(node, fieldName);
  }

  protected static String fetchFormattedEventDate(ObjectNode node, String fieldName) {
    if (node != null && node.has(fieldName)) {
      Date time = new Date(node.get(fieldName).asLong());
      return dateFormat.format(time);
    }

    return "";
  }

  protected static String fetchEventType(ObjectNode node) {
    if (node != null && node.has("name")) {
      String name = node.get("name").asText();
      if (name.startsWith("_")) {
        name = "tk" + name;
      }

      return name;
    }

    return "_unknown_event_type";
  }

  protected static String getDateFromSinkRecord(SinkRecord record) {
    return dateFormat.format(record.timestamp());
  }
}
