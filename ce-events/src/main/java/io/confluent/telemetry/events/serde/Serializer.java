/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import io.cloudevents.CloudEvent;
import io.cloudevents.format.Wire;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.v03.AttributesImpl;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * A class that serializes a cloudevent for Kafka transport.
 *
 * Note that this does not implement the Kafka Serializer interface because it does not allow us to
 * create a ProducerRecord with the correct headers and timestamp.
 **/
public abstract class Serializer<T> {

  protected EventStep<AttributesImpl, T, byte[], byte[]> builder;

  private Wire<byte[], String, byte[]> marshal(Supplier<CloudEvent<AttributesImpl, T>> event,
      EventStep<AttributesImpl, T, byte[], byte[]> builder) {
    return Optional.ofNullable(builder)
        .map(step -> step.withEvent(event))
        .map(marshaller -> marshaller.marshal())
        .get();

  }

  public byte[] serialize(CloudEvent<AttributesImpl, T> event) {
    Wire<byte[], String, byte[]> wire = marshal(() -> event, this.builder);

    if (!wire.getPayload().isPresent()) {
      throw new RuntimeException("payload is empty");
    }

    return wire.getPayload().get();
  }

  public String toString(CloudEvent<AttributesImpl, T> event) {
    return new String(serialize(event));
  }

  private Set<Header> marshalHeaders(Map<String, byte[]> headers) {

    return headers.entrySet()
        .stream()
        .map(header -> new RecordHeader(header.getKey(), header.getValue()))
        .collect(Collectors.toSet());

  }

  public ProducerRecord<String, byte[]> producerRecord(CloudEvent<AttributesImpl, T> event,
      String topic, Integer partition) {

    Wire<byte[], String, byte[]> wire = marshal(() -> event, this.builder);
    Set<Header> headers = marshalHeaders(wire.getHeaders());

    Long timestamp = null;
    if (event.getAttributes().getTime().isPresent()) {
      timestamp = event.getAttributes().getTime().get().toInstant().toEpochMilli();
    }

    if (!wire.getPayload().isPresent()) {
      throw new RuntimeException("payload is empty");
    }

    byte[] payload = wire.getPayload().get();

    return new ProducerRecord<String, byte[]>(
        topic,
        partition,
        timestamp,
        // Get partitionKey from cloudevent extensions once it is supported upstream.
        null,
        payload,
        headers);
  }


}
