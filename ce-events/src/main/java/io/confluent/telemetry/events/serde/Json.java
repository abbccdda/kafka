/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.cloudevents.json.ZonedDateTimeDeserializer;
import io.cloudevents.json.ZonedDateTimeSerializer;
import io.cloudevents.v03.CloudEventImpl;
import java.time.ZonedDateTime;


/**
 * Utility class to create the JSON serializers and deserializers.
 **/
public class Json {

  public static final String APPLICATION_JSON = "application/json";

  // Setup Jackson correctly.
  private static ObjectMapper createJacksonMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());

    // add Jackson datatype for ZonedDateTime
    final SimpleModule module = new SimpleModule();
    module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
    module.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
    mapper.registerModule(module);
    return mapper;
  }

  /**
   * Get a {@link Serializer} which can serialize cloudevents using binary encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#32-binary-content-mode
   *
   * @return {@link BinarySerializer}
   */
  public static <T> Serializer<T> binarySerializer() {
    return new BinarySerializer<T>((data, headers) -> uncheckedEncode(createJacksonMapper(), data));
  }

  /**
   * Get a {@link Deserializer} which can deserialize cloudevents using binary encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#32-binary-content-mode
   *
   * @return {@link BinaryDeserializer}
   */
  public static <T> Deserializer<T> binaryDeserializer(Class<T> typeParameterClass) {

    ObjectMapper mapper = createJacksonMapper();
    return new BinaryDeserializer<>(APPLICATION_JSON, (payload, att) -> {
      if (payload == null) {
        return null;
      }
      try {
        return mapper.readValue(payload, typeParameterClass);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to decode: " + e.getMessage());
      }
    });
  }

  /**
   * Get a {@link Serializer} which can serialize cloudevents using structured encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#33-structured-content-mode
   *
   * @return {@link StructuredSerializer}
   */
  public static <T> Serializer<T> structuredSerializer() {
    return new StructuredSerializer<T>(event -> uncheckedEncode(createJacksonMapper(), event));
  }

  // Wrap the checked exception in an unchecked one for use in a lambda function.
  private static byte[] uncheckedEncode(ObjectMapper mapper, final Object obj) {
    try {
      return mapper.writeValueAsBytes(obj);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to encode as JSON: " + e.getMessage());
    }
  }

  /**
   * Get a {@link Deserializer} which can serialize cloudevents using structured encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#33-structured-content-mode
   *
   * @return {@link StructuredDeserializer}
   */
  public static <T> Deserializer<T> structuredDeserializer(Class<T> typeParameterClass) {
    ObjectMapper mapper = createJacksonMapper();
    return new StructuredDeserializer<T>(payload -> {
      if (payload == null) {
        return null;
      }
      try {
        JavaType type = mapper.getTypeFactory()
            .constructParametricType(CloudEventImpl.class, typeParameterClass);
        return mapper.readValue(payload, type);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to decode: " + e.getMessage(), e);
      }
    });
  }

}
