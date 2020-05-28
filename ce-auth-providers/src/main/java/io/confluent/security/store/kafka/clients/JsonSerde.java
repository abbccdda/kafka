// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.security.authorizer.utils.JsonMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T>, Configurable {

  private final Class<T> clazz;

  private JsonSerde(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(configs);
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public Serializer<T> serializer() {
    return this;
  }

  @Override
  public Deserializer<T> deserializer() {
    return this;
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data != null) {
      try {
        return JsonMapper.objectMapper().writeValueAsBytes(data);
      } catch (JsonProcessingException e) {
        throw new SerializationException("Data could not be serialized for topic " + topic, e);
      }
    } else {
      return null;
    }
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data != null) {
      try {
        return JsonMapper.objectMapper().readValue(data, clazz);
      } catch (IOException e) {
        throw new SerializationException("Data could not be deserialized for topic " + topic, e);
      }
    } else {
      return null;
    }
  }

  @Override
  public void close() {
  }

  public static <T> JsonSerde<T> serde(Class<T> clazz, boolean isKey) {
    JsonSerde<T> serde = new JsonSerde<T>(clazz);
    serde.configure(Collections.emptyMap(), isKey);
    return serde;
  }
}
