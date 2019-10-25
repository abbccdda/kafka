/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events;

import io.cloudevents.CloudEvent;
import io.confluent.events.cloudevents.kafka.Marshallers;
import java.util.Optional;

public class CloudEventUtils {


  /**
   * Return the JSON string for this CloudEvent
   */
  // TODO(sumit): rename this to: toStructuredString
  public static String toJsonString(CloudEvent event) {
    Optional<byte[]> b = Marshallers.structuredProto().withEvent(() -> event).marshal()
        .getPayload();

    return b.map(String::new).orElse(null);
  }
}
