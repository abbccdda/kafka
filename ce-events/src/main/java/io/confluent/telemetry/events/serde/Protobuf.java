/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import static io.confluent.telemetry.events.EventLoggerConfig.CLOUD_EVENT_BINARY_ENCODING;
import static io.confluent.telemetry.events.EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.cloudevents.json.ZonedDateTimeDeserializer;
import io.cloudevents.v03.CloudEventImpl;
import java.time.ZonedDateTime;

/**
 * Utility class to create the Protobuf serializers and deserializers.
 **/
public class Protobuf {

 // Setup Jackson correctly.
  private static ObjectMapper createJacksonMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());

    // add Jackson datatype for ZonedDateTime
    final SimpleModule module = new SimpleModule();
    module.addSerializer(ZonedDateTime.class, new ZonedDateTimeMillisSerializer());
    module.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
    mapper.registerModule(module);

    mapper.registerModule(new ProtobufModule());
    return mapper;
  }

  /**
   * Get a {@link Serializer} which can serialize cloudevents with Protobuf payload using binary
   * encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#32-binary-content-mode
   * @return  {@link BinarySerializer}
   */
  public static <T extends MessageLite> Serializer<T> binarySerializer() {
    return new BinarySerializer<T>((data, headers) -> data.toByteArray());
  }

  /**
   * Get a {@link Deserializer} which can deserialize cloudevents with Protobuf payload using binary
   * encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#32-binary-content-mode
   * @return  {@link BinaryDeserializer}
   */
  public static <T extends MessageLite> Deserializer<T> binaryDeserializer(Parser<T> parser) {
    return new BinaryDeserializer<>(APPLICATION_PROTOBUF, (data, att) -> {
      if (data == null) {
        return null;
      }
      try {
        return parser.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Get a {@link Serializer} which can serialize cloudevents with Protobuf payload using
   * structured encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#33-structured-content-mode
   * @return  {@link StructuredSerializer}
   */
  public static <T extends MessageLite> Serializer<T> structuredSerializer() {
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
   * Get a {@link Deserializer} which can serialize cloudevents with Protobuf payload using
   * structured encoding.
   * https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#33-structured-content-mode
   * @return  {@link StructuredDeserializer}
   */
  public static <T extends MessageLite> Deserializer<T> structuredDeserializer(
      Class<T> typeParameterClass) {
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

  public static final String APPLICATION_PROTOBUF = "application/x-protobuf";
  public static final String APPLICATION_JSON = "application/json";

  /**
   * Get content type based on encoding
   * @param encoding (Either structured or binary)
   * @return content type
   */
  public static String contentType(String encoding) {
    switch (encoding) {
      case CLOUD_EVENT_BINARY_ENCODING:
        return APPLICATION_PROTOBUF;
      case CLOUD_EVENT_STRUCTURED_ENCODING:
        return APPLICATION_JSON;
      default:
        throw new RuntimeException("Invalid cloudevent encoding: " + encoding);
    }
  }

  // For testing only
  public static <T extends MessageLite>  Deserializer<T> deserializer(String encoding, Class<T> clazz, Parser<T> parser) {
    switch (encoding) {
      case CLOUD_EVENT_BINARY_ENCODING:
        return Protobuf.binaryDeserializer(parser);
      case CLOUD_EVENT_STRUCTURED_ENCODING:
        return Protobuf.structuredDeserializer(clazz);
      default:
        throw new RuntimeException("Invalid cloudevent encoding: " + encoding);
    }
  }
}