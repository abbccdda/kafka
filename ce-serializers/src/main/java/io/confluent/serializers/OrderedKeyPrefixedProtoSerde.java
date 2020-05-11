/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.Map;

public class OrderedKeyPrefixedProtoSerde<
      E extends Enum<E> & ProtocolMessageEnum,
      T extends Message
    > extends OrderedKeyProtoSerde<T> implements OrderedKeyPrefixedSerde<E, T> {

  private E prefix;
  private ImmutableMap<E, ImmutableList<FieldDescriptor>> fieldMap;

  private static <E extends Enum<E> & ProtocolMessageEnum, T extends Message>
      ImmutableMap<E, ImmutableList<FieldDescriptor>> getFieldMap(
        T instance,
        ImmutableMap<E, ImmutableList<Integer>> fieldIdsMap
  ) {
    Descriptor desc = instance.getDescriptorForType();
    ImmutableMap.Builder<E, ImmutableList<FieldDescriptor>> mapBuilder = ImmutableMap.builder();
    for (Map.Entry<E, ImmutableList<Integer>> entry : fieldIdsMap.entrySet()) {
      ImmutableList.Builder<FieldDescriptor> listBuilder = ImmutableList.builder();
      for (int fieldNumber : entry.getValue()) {
        listBuilder.add(desc.findFieldByNumber(fieldNumber));
      }
      mapBuilder.put(entry.getKey(), listBuilder.build());
    }
    return mapBuilder.build();
  }

  public static <E extends Enum<E> & ProtocolMessageEnum, T extends Message>
      OrderedKeyPrefixedSerde<E, T> create(
        E prefix,
        T instance,
        ImmutableMap<E, ImmutableList<Integer>> fieldIdsMap
  ) {
    return new OrderedKeyPrefixedProtoSerde<>(prefix, instance, getFieldMap(instance, fieldIdsMap));
  }

  public OrderedKeyPrefixedProtoSerde(
      E prefix,
      T instance,
      ImmutableMap<E, ImmutableList<FieldDescriptor>> fieldMap
  ) {
    super(instance, fieldMap.get(prefix));
    this.prefix = prefix;
    this.fieldMap = fieldMap;
  }

  public OrderedKeyPrefixedProtoSerde(
      E prefix,
      T instance,
      ImmutableMap<E, ImmutableList<FieldDescriptor>> fieldMap,
      int numFields
  ) {
    super(instance, fieldMap.get(prefix).subList(0, numFields));
    this.prefix = prefix;
    this.fieldMap = fieldMap;
  }

  @Override
  public E prefix() {
    return prefix;
  }

  @Override
  public E extractPrefix(Bytes key) {
    ByteBuffer buffer = ByteBuffer.wrap(key.get());
    readMagicByte(buffer);
    return readPrefix(buffer);
  }

  protected E readPrefix(ByteBuffer buffer) {
    Short num = OrderedBytes.readShort(buffer);
    if (num == null) {
      return null;
    }
    @SuppressWarnings("unchecked")
    E value = (E) E.valueOf(
        prefix.getClass(),
        prefix.getDescriptorForType().findValueByNumber(num).getName());
    return value;
  }

  @Override
  public OrderedKeyPrefixedSerde<E, T> prefixKeySerde(int numFields) {
    if (numFields <= 0) {
      throw new IllegalArgumentException(
          String.format("Fields requested %d must be greater than zero", numFields)
      );
    }
    if (numFields > fields.size()) {
      throw new IllegalArgumentException(
          String.format("Fields requested %d is higher than total %d", numFields, fields.size())
      );
    }
    return new OrderedKeyPrefixedProtoSerde<>(prefix, instance, fieldMap, numFields);
  }

  @Override
  public Bytes key(T message) {
    return Bytes.wrap(protoToBytes(message, fields, fields.size()));
  }

  @Override
  protected byte[] protoToBytes(
      T message,
      ImmutableList<FieldDescriptor> fields,
      int numFields
  ) {
    if (message == null) {
      return null;
    }
    try {
      ByteBuffer buffer = ByteBuffer.allocate(
          ENUM_FIELD_SIZE + getMaxSerializedBytes(message, fields)
      );
      writeMagicByte(buffer);
      encodeEnum(buffer, prefix.getNumber());
      writeProto(buffer, message, fields, fields.size());
      byte[] result = new byte[buffer.position()];
      buffer.flip();
      buffer.get(result, 0, result.length);
      return result;
    } catch (Exception e) {
      String errMsg = "Error serializing key message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public T toProto(Bytes key) {
    if (key == null) {
      return null;
    }
    try {
      ByteBuffer buffer = ByteBuffer.wrap(key.get());
      readMagicByte(buffer);
      E msgPrefix = readPrefix(buffer);
      ImmutableList<FieldDescriptor> msgFields = fieldMap.get(msgPrefix);
      return readProto(buffer, instance, msgFields);
    } catch (SerializationException e) {
      throw e;
    } catch (Exception e) {
      String errMsg = "Error deserializing key message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public byte[] fromJson(String json) {
    Gson gson = new Gson();
    try {
      JsonObject root = JsonParser.parseString(json).getAsJsonObject();
      JsonElement protoJson = root.get("proto");
      String protoJsonStr = gson.toJson(protoJson);
      String prefixStr = root.get("prefix").getAsString();
      @SuppressWarnings("unchecked")
      E msgPrefix = (E) E.valueOf(prefix.getClass(), prefixStr);
      ImmutableList<FieldDescriptor> msgFields = fieldMap.get(msgPrefix);
      T msg = jsonToProto(protoJsonStr, instance);
      return protoToBytes(msg, msgFields, msgFields.size());
    } catch (SerializationException e) {
      throw e;
    } catch (Exception e) {
      throw new SerializationException("JSON parsing failed", e);
    }
  }

  @Override
  public String toJson(Bytes key) {
    Gson gson = new Gson();
    E msgPrefix = extractPrefix(key);
    T msg = toProto(key);
    JsonObject root = new JsonObject();
    root.add("prefix", new JsonPrimitive(msgPrefix.getValueDescriptor().getName()));
    root.add("proto", JsonParser.parseString(protoToJson(msg)));
    return gson.toJson(root);
  }
}
