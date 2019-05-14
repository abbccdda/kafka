/*
 * Copyright [2016 - 2016] Confluent Inc.
 */
package io.confluent.serializers;

import com.google.gson.Gson;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.Before;
import org.junit.Test;

import io.confluent.serializers.record.Test.AMessage;
import io.confluent.serializers.record.Test.AnEnum;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ProtoSerdeTest {

  private ProtoSerde<AMessage> serde;
  private static final String JSON = "{\"aString\":\"my string\",\"aLong\":\"1029309\",\"second_string\":\"second string\"}";
  private final AMessage original = AMessage.newBuilder()
      .setALong(1029309)
      .setAString("my string")
      .setAnEnum(AnEnum.A)
      .setSecondString("second string")
      .build();

  @Before
  public void before() {
    serde = new ProtoSerde<>(AMessage.getDefaultInstance());
  }

  @Test
  public void testBasicSerde() {
    byte[] serialized = serde.serialize(original);
    AMessage deserialized = serde.deserialize(serialized);
    assertEquals(original, deserialized);
  }

  @Test
  public void testSerializingNull() {
    byte[] serialized = serde.serialize(null);
    assertNull(serialized);
  }

  @Test
  public void testDeserializingNull() {
    AMessage desrialized = serde.deserialize(null);
    assertNull(desrialized);
  }

  @Test(expected = SerializationException.class)
  public void testDeserializeWrongMagicMyte() {
    byte[] badBytes = new byte[] {UberSerde.MAGIC_BYTE_PROTOBUF - 1, 'h', 'e', 'l', 'l', 'o'};
    serde.deserialize(badBytes);
  }

  @Test
  public void testMagicByteIsSet() {
    byte[] serialized = serde.serialize(original);
    assertEquals(ProtoSerde.MAGIC_BYTE_PROTOBUF, serialized[0]);
  }

  @Test
  public void fromJSON() throws Exception {
    byte[] bytes = serde.fromJson(JSON);
    assertArrayEquals(serde.serialize(original), bytes);
  }

  @Test(expected = SerializationException.class)
  public void testFromInvalidJson() {
    serde.fromJson("this is no good" + JSON);
  }

  @Test
  public void toJSON() throws Exception {
    String json = serde.toJson(original);
    assertFalse(json.contains("\n"));
    Gson gson = new Gson();
    Map<String, Object> decoded = (Map<String, Object>) gson.fromJson(json, HashMap.class);
    assertEquals("my string", decoded.get("aString"));
  }

  @Test
  public void toJSONPreseveFieldNameTrue() throws Exception {
    String json = serde.toJson(original, true);
    assertFalse(json.contains("\n"));
    Gson gson = new Gson();
    Map<String, Object> decoded = (Map<String, Object>) gson.fromJson(json, HashMap.class);
    assertEquals("second string", decoded.get("second_string"));
  }

  @Test
  public void toJSONPreseveFieldNameFalse() throws Exception {
    String json = serde.toJson(original, false);
    assertFalse(json.contains("\n"));
    Gson gson = new Gson();
    Map<String, Object> decoded = (Map<String, Object>) gson.fromJson(json, HashMap.class);
    assertEquals("second string", decoded.get("secondString"));
  }
}
