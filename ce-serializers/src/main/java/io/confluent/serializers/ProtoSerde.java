/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.util.JsonFormat;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class ProtoSerde<T extends Message> implements
                                                      UberSerde<T>,
                                                      Serde<T>,
                                                      Deserializer<T>,
                                                      Serializer<T> {

  private static final Logger log = LoggerFactory.getLogger(ProtoSerde.class);
  private final Parser<T> parser;
  private final T instance;

  @SuppressWarnings("unchecked")
  public ProtoSerde(T instance) {
    this.instance = instance;
    this.parser = (Parser<T>) instance.getParserForType();
  }

  @SuppressWarnings("unchecked")
  public Class<T> type() {
    return (Class<T>) instance.getClass();
  }

  public byte[] serialize(T message) {
    return serializeUntyped(message);
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return this.serialize(data);
  }

  private byte[] serializeUntyped(Message message) {
    if (message == null) {
      return null;
    }
    try {
      ByteBuffer buffer = ByteBuffer.allocate(message.getSerializedSize() + 1);
      CodedOutputStream cos = CodedOutputStream.newInstance(buffer);
      cos.writeRawByte(MAGIC_BYTE_PROTOBUF);
      message.writeTo(cos);
      cos.flush();
      return buffer.array();
    } catch (Exception e) {
      String errMsg = "Error serializing protobuf message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public T deserialize(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      CodedInputStream cis = CodedInputStream.newInstance(buffer);
      byte magic = cis.readRawByte();
      if (MAGIC_BYTE_PROTOBUF != magic) {
        String errMsg = String.format(
            "Tried to deserialize message with unknown magic byte %s",
            magic
        );
        log.error(errMsg);
        throw new SerializationException(errMsg);
      }
      return parser.parseFrom(cis);
    } catch (SerializationException e) {
      throw e;
    } catch (Exception e) {
      String errMsg = "Error deserializing protobuf message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    return this.deserialize(data);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public void close() {
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
  public byte[] fromJson(String json) {
    Message.Builder builder = instance.newBuilderForType();
    try {
      JsonFormat.parser().merge(json, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException("JSON parsing failed: " + e.getMessage(), e);
    }
    return serializeUntyped(builder.build());
  }

  @Override
  public String toJson(T obj) {
    return toJson(obj, false);
  }

  public String toJson(T obj, boolean preseveFieldName) {
    try {
      if (preseveFieldName) {
        return JsonFormat.printer()
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .print(obj);
      } else {
        return JsonFormat.printer()
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace()
            .print(obj);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException("JSON formatting failed: " + e.getMessage(), e);
    }
  }
}
