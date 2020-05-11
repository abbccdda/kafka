/*
 * Copyright [2016 - 2016] Confluent Inc.
 */
package io.confluent.serializers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.confluent.serializers.record.Test.AMessage;
import io.confluent.serializers.record.Test.AnEnum;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class OrderedKeyPrefixedProtoSerdeTest {

  private final static ImmutableMap<AnEnum, ImmutableList<Integer>> FIELDMAP = ImmutableMap.<AnEnum, ImmutableList<Integer>>builder()
      .put(AnEnum.A, ImmutableList.of(AMessage.ALONG_FIELD_NUMBER, AMessage.ASTRING_FIELD_NUMBER))
      .put(AnEnum.B, ImmutableList.of(AMessage.ASTRING_FIELD_NUMBER))
      .build();
  private final static String A_STRING = "flubs";
  private final static AMessage ORIGINAL = AMessage.newBuilder()
      .setAString(A_STRING)
      .build();
  private OrderedKeyPrefixedSerde<AnEnum, AMessage> serde;

  @Before
  public void before() {
    serde = OrderedKeyPrefixedProtoSerde.create(AnEnum.B, AMessage.getDefaultInstance(), FIELDMAP);
  }


  @Test
  public void testSerializationFormat() {
    assertEquals(1, serde.numFields());
    Bytes key = serde.key(ORIGINAL);
    byte[] serialized = serde.serialize(key);

    int offset = 0;
    assertEquals(OrderedKeyProtoSerde.MAGIC_BYTE_ORDERED_KEY, serialized[offset]);
    offset += 1;

    assertEquals(OrderedBytes.MARKER_START_FIXED_INT_16, serialized[offset]);
    offset += 1;
    byte[] enumBytes = ByteBuffer.allocate(Short.SIZE / Byte.SIZE).order(ByteOrder.BIG_ENDIAN).putShort(OrderedBytes.flipSignBit((short) AnEnum.B.getValueDescriptor().getNumber())).array();
    assertArrayEquals(enumBytes, Arrays.copyOfRange(serialized, offset, offset + enumBytes.length));
    offset += enumBytes.length;

    assertEquals(OrderedBytes.MARKER_START_STRING, serialized[offset]);
    offset += 1;
    byte[] aStrBytes = A_STRING.getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(aStrBytes, Arrays.copyOfRange(serialized, offset, offset + aStrBytes.length));
    offset += aStrBytes.length;
    assertEquals(OrderedBytes.MARKER_END_STRING, serialized[offset]);
    offset += 1;

    assertEquals("End of message", serialized.length, offset);

    assertEquals("7E2A800134666C75627300", serde.toHexString(Bytes.wrap(serialized)));

    AMessage deserialized = serde.toProto(Bytes.wrap(serialized));
    assertEquals(ORIGINAL, deserialized);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrefixedSerdeRequiresSomeFields() {
    serde.prefixKeySerde(serde.numFields() - 1);
  }

  @Test
  public void testPrefixedSerde() {
    OrderedKeyPrefixedSerde<AnEnum, AMessage> aSerde = OrderedKeyPrefixedProtoSerde.create(AnEnum.A, AMessage.getDefaultInstance(), FIELDMAP);
    assertEquals(2, aSerde.numFields());
    OrderedKeyPrefixedSerde<AnEnum, AMessage> prefixedSerde = aSerde.prefixKeySerde(aSerde.numFields() - 1);
    assertEquals(1, prefixedSerde.numFields());
    OrderedBytes.startsWith(aSerde.key(ORIGINAL).get(), prefixedSerde.key(ORIGINAL).get());
  }

  @Test
  public void testExtractPrefix() {
    Bytes key = serde.key(ORIGINAL);
    AnEnum prefix = serde.extractPrefix(key);
    assertEquals(AnEnum.B, prefix);

    OrderedKeyPrefixedSerde<AnEnum, AMessage> aPrefixedSerde = OrderedKeyPrefixedProtoSerde.create(AnEnum.A, AMessage.getDefaultInstance(), FIELDMAP);
    assertEquals(AnEnum.A, aPrefixedSerde.prefix());
    AnEnum prefixFromA = aPrefixedSerde.extractPrefix(key);
    assertEquals(AnEnum.B, prefixFromA);
  }

  @Test
  public void testFromJSON() {
    String json = "{\"prefix\":\"B\",\"proto\": {\"aString\":\"flubs\"}}";
    byte[] bytes = serde.fromJson(json);
    assertEquals(serde.key(ORIGINAL), Bytes.wrap(bytes));
  }

  @Test
  public void testToJSON() throws Exception {
    String json = serde.toJson(serde.key(ORIGINAL));
    validateJSON(json);
  }

  @Test
  public void testDefaultSerdeToJSON() {
    OrderedKeyPrefixedSerde<AnEnum, AMessage> defaultSerde = OrderedKeyPrefixedProtoSerde.create(AnEnum.UNRECOGNIZED, AMessage.getDefaultInstance(), FIELDMAP);
    String json = defaultSerde.toJson(serde.key(ORIGINAL));
    validateJSON(json);
  }

  private void validateJSON(String json) {
    assertFalse(json.contains("\n"));
    Gson gson = new Gson();
    Map<String, Object> decoded = (Map<String, Object>) gson.fromJson(json, HashMap.class);
    assertEquals("B", decoded.get("prefix"));
    Map<String, Object> protoMap = (Map<String, Object>) decoded.get("proto");
    assertNotNull(protoMap);
    assertEquals("flubs", protoMap.get("aString"));
  }

}
