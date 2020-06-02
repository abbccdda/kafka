/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import io.cloudevents.CloudEvent;
import io.cloudevents.format.builder.HeadersStep;
import io.cloudevents.v03.AttributesImpl;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * A class that deserializes a cloudevent from a Kafka message.
 *
 * Note that this does not implement the Kafka Deserializer interface because we need the entire
 * ConsumerRecord to decode the cloudevent message properly.
 */

public abstract class Deserializer<T> {

  protected HeadersStep<AttributesImpl, T, byte[]> builder;

  public CloudEvent<AttributesImpl, T> deserialize(Headers headers, byte[] data) {
    return builder
        .withHeaders(() -> asMap(headers))
        .withPayload(() -> data)
        .unmarshal();
  }

  public CloudEvent<AttributesImpl, T> deserialize(ConsumerRecord<?, byte[]> record) {
    return builder
        .withHeaders(() -> asMap(record.headers()))
        .withPayload(record::value)
        .unmarshal();
  }

  private static Map<String, Object> asMap(Headers kafkaHeaders) {
    return StreamSupport.stream(kafkaHeaders.spliterator(), Boolean.FALSE)
        .map(header -> new SimpleEntry<String, Object>(header.key(), header.value()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

}
