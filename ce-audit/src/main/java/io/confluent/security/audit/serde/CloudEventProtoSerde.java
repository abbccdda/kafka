package io.confluent.security.audit.serde;

import io.confluent.security.audit.CloudEvent;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CloudEventProtoSerde implements Serde<CloudEvent>, Serializer<CloudEvent>,
    Deserializer<CloudEvent> {

  @Override
  public CloudEvent deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return CloudEvent.parseFrom(data);
    } catch (SerializationException e) {
      throw e;
    } catch (Exception e) {
      String errMsg = "Error deserializing protobuf message";
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public CloudEvent deserialize(String topic, Headers headers, byte[] data) {
    return deserialize(topic, data);
  }

  @Override
  public byte[] serialize(String topic, CloudEvent data) {
    return data.toByteArray();
  }

  @Override
  public byte[] serialize(String topic, Headers headers, CloudEvent data) {
    return serialize(topic, data);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public void close() {

  }

  @Override
  public Serializer<CloudEvent> serializer() {
    return this;
  }

  @Override
  public Deserializer<CloudEvent> deserializer() {
    return this;
  }

}
