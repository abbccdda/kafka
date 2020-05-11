/*
 * Copyright [2016 - 2016] Confluent Inc.
 */
package io.confluent.serializers;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;

import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.confluent.serializers.record.Test.AMessage;
import io.confluent.serializers.record.Test.AnEnum;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class OrderedKeyProtoSerdeTest {

  private final static Long A_LONG = 1029309L;
  private final static String A_STRING = "my string";
  private final static String ANOTHER_STRING = "Another one";
  private final static AMessage ORIGINAL = AMessage.newBuilder()
      .setALong(A_LONG)
      .setAString(A_STRING)
      .setAnEnum(AnEnum.B)
      .setAnotherString(ANOTHER_STRING)
      .build();
  private OrderedKeyProtoSerde<AMessage> serde;

  @Before
  public void before() {
    serde = OrderedKeyProtoSerde.create(AMessage.getDefaultInstance(),
        AMessage.ANOTHERSTRING_FIELD_NUMBER,
        AMessage.ASTRING_FIELD_NUMBER,
        AMessage.ALONG_FIELD_NUMBER,
        AMessage.ANENUM_FIELD_NUMBER
    );
  }

  @Test
  public void testSerializationFormat() {
    Bytes key = serde.key(ORIGINAL);
    byte[] serialized = serde.serialize(key);

    int offset = 0;
    assertEquals(OrderedKeyProtoSerde.MAGIC_BYTE_ORDERED_KEY, serialized[offset]);
    offset += 1;

    assertEquals(OrderedBytes.MARKER_START_STRING, serialized[offset]);
    offset += 1;
    byte[] anotherStrBytes = ANOTHER_STRING.getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(anotherStrBytes, Arrays.copyOfRange(serialized, offset, offset + anotherStrBytes.length));
    offset += anotherStrBytes.length;
    assertEquals(OrderedBytes.MARKER_END_STRING, serialized[offset]);
    offset += 1;

    assertEquals(OrderedBytes.MARKER_START_STRING, serialized[offset]);
    offset += 1;
    byte[] aStrBytes = A_STRING.getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(aStrBytes, Arrays.copyOfRange(serialized, offset, offset + aStrBytes.length));
    offset += aStrBytes.length;
    assertEquals(OrderedBytes.MARKER_END_STRING, serialized[offset]);
    offset += 1;

    assertEquals(OrderedBytes.MARKER_START_FIXED_INT_64, serialized[offset]);
    offset += 1;
    byte[] aLongBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.BIG_ENDIAN).putLong(OrderedBytes.flipSignBit(A_LONG)).array();
    assertArrayEquals(aLongBytes, Arrays.copyOfRange(serialized, offset, offset + aLongBytes.length));
    offset += aLongBytes.length;

    assertEquals(OrderedBytes.MARKER_START_FIXED_INT_16, serialized[offset]);
    offset += 1;
    byte[] enumBytes = ByteBuffer.allocate(Short.SIZE / Byte.SIZE).order(ByteOrder.BIG_ENDIAN).
        putShort(OrderedBytes.flipSignBit((short) AnEnum.B.getNumber())).array();
    assertArrayEquals(enumBytes, Arrays.copyOfRange(serialized, offset, offset + enumBytes.length));
    offset += enumBytes.length;

    assertEquals("End of message", serialized.length, offset);

    assertEquals("7E34416E6F74686572206F6E6500346D7920737472696E67002C80000000000FB4BD2A8001", serde.toHexString(Bytes.wrap(serialized)));

    AMessage deserialized = serde.toProto(Bytes.wrap(serialized));
    assertEquals(ORIGINAL, deserialized);
  }

  @Test
  public void testDefaults() {
    AMessage defaults = AMessage.newBuilder().build();
    byte[] serialized = serde.serialize(serde.key(defaults));
    final byte[] emptyStrBytes = "".getBytes(StandardCharsets.UTF_8);

    int offset = 0;
    assertEquals(OrderedKeyProtoSerde.MAGIC_BYTE_ORDERED_KEY, serialized[offset]);
    offset += 1;

    assertEquals(OrderedBytes.MARKER_START_STRING, serialized[offset]);
    offset += 1;
    assertArrayEquals(emptyStrBytes, Arrays.copyOfRange(serialized, offset, offset + emptyStrBytes.length));
    offset += emptyStrBytes.length;
    assertEquals(OrderedBytes.MARKER_END_STRING, serialized[offset]);
    offset += 1;

    assertEquals(OrderedBytes.MARKER_START_STRING, serialized[offset]);
    offset += 1;
    assertArrayEquals(emptyStrBytes, Arrays.copyOfRange(serialized, offset, offset + emptyStrBytes.length));
    offset += emptyStrBytes.length;
    assertEquals(OrderedBytes.MARKER_END_STRING, serialized[offset]);
    offset += 1;

    assertEquals(OrderedBytes.MARKER_START_FIXED_INT_64, serialized[offset]);
    offset += 1;
    byte[] aLongBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.BIG_ENDIAN).
        putLong(OrderedBytes.flipSignBit(0L)).array();
    assertArrayEquals(aLongBytes, Arrays.copyOfRange(serialized, offset, offset + aLongBytes.length));
    offset += aLongBytes.length;

    assertEquals(OrderedBytes.MARKER_START_FIXED_INT_16, serialized[offset]);
    offset += 1;
    byte[] enumBytes = ByteBuffer.allocate(Short.SIZE / Byte.SIZE).order(ByteOrder.BIG_ENDIAN).
        putShort(OrderedBytes.flipSignBit((short) 0)).array();
    assertArrayEquals(enumBytes, Arrays.copyOfRange(serialized, offset, offset + enumBytes.length));
    offset += enumBytes.length;

    assertEquals("End of message", serialized.length, offset);

    assertEquals("7E340034002C80000000000000002A8000", serde.toHexString(Bytes.wrap(serialized)));

    AMessage deserialized = serde.toProto(Bytes.wrap(serialized));
    assertEquals(defaults, deserialized);
  }

  @Test
  public void testStringSort() {
    Descriptors.Descriptor desc = AMessage.getDescriptor();
    ImmutableList<FieldDescriptor> fields = ImmutableList.of(
        desc.findFieldByNumber(AMessage.ASTRING_FIELD_NUMBER),
        desc.findFieldByNumber(AMessage.ANOTHERSTRING_FIELD_NUMBER)
    );
    serde = new OrderedKeyProtoSerde<>(AMessage.getDefaultInstance(), fields);
    //Test that a record with a shorter key in the first field always comes before a longer one
    //Period is decimal 46 in UTF-8 and comes before any letters or colon - this test would fail with : as separator
    Bytes shortFirstWithLowerSecond = serde.key(AMessage.newBuilder().setAString("a").setAnotherString("zzzzzz").build());
    Bytes longerFirstWithHigherSecond = serde.key(AMessage.newBuilder().setAString("a.zzzzz").setAnotherString("z").build());
    assertTrue("Strings sort correctly", shortFirstWithLowerSecond.compareTo(longerFirstWithHigherSecond) < 0);

    byte[] shortFirstWithLowerSecondStr = Joiner.on(":").join("a", "zzzzzz").getBytes(StandardCharsets.UTF_8);
    byte[] longerFirstWithHigherSecondStr = Joiner.on(":").join("a.zzzzz", "z").getBytes(StandardCharsets.UTF_8);
    assertFalse("Strings don't sort correctly with colon separator", Bytes.BYTES_LEXICO_COMPARATOR.compare(shortFirstWithLowerSecondStr, longerFirstWithHigherSecondStr) < 0);
  }

  @Test
  public void testLongSort() {
    Descriptors.Descriptor desc = AMessage.getDescriptor();
    ImmutableList<FieldDescriptor> fields = ImmutableList.of(desc.findFieldByNumber(AMessage.ALONG_FIELD_NUMBER));
    serde = new OrderedKeyProtoSerde<>(AMessage.getDefaultInstance(), fields);
    Bytes smaller = serde.key(AMessage.newBuilder().setALong(1L).build());
    Bytes bigger = serde.key(AMessage.newBuilder().setALong(2048L).build());
    assertTrue("Longs sort correctly", smaller.compareTo(bigger) < 0);


    Set<Bytes> messages = Sets.newTreeSet();
    for (long l : new Long[]{-100L, 0L, 100L}) {
      messages.add(
          serde.key(
              AMessage.newBuilder()
                  .setALong(l)
                  .setAnEnum(ORIGINAL.getAnEnum())
                  .build()
          )
      );
    }
    long last = Long.MIN_VALUE;
    for (Bytes b : messages) {
      AMessage message = serde.toProto(b);
      assertTrue(message.getALong() > last);
      last = message.getALong();
    }
  }

  @Test
  public void testIntSort() {
    Descriptors.Descriptor desc = AMessage.getDescriptor();
    ImmutableList<FieldDescriptor> fields = ImmutableList.of(desc.findFieldByNumber(AMessage.ANINT_FIELD_NUMBER));
    serde = new OrderedKeyProtoSerde<>(AMessage.getDefaultInstance(), fields);
    Bytes smaller = serde.key(AMessage.newBuilder().setAnInt(1).build());
    Bytes bigger = serde.key(AMessage.newBuilder().setAnInt(2048).build());
    assertTrue("Longs sort correctly", smaller.compareTo(bigger) < 0);


    Set<Bytes> messages = Sets.newTreeSet();
    for (int l : new Integer[]{-100, 0, 100}) {
      messages.add(
          serde.key(
              AMessage.newBuilder()
                  .setAnInt(l)
                  .setAnEnum(ORIGINAL.getAnEnum())
                  .build()
          )
      );
    }
    int last = Integer.MIN_VALUE;
    for (Bytes b : messages) {
      AMessage message = serde.toProto(b);
      assertTrue(message.getAnInt() > last);
      last = message.getAnInt();
    }
  }

  @Test
  public void testFromJSON() {
    String json = "{\"aString\":\"my string\",\"anotherString\":\"Another one\",\"aLong\":\"1029309\",\"anEnum\":\"B\"}";
    byte[] bytes = serde.fromJson(json);
    assertEquals(serde.key(ORIGINAL), Bytes.wrap(bytes));
  }

  @Test
  public void testToJSON() throws Exception {
    String json = serde.toJson(serde.key(ORIGINAL));
    assertFalse(json.contains("\n"));
    Gson gson = new Gson();
    Map<String, Object> decoded = (Map<String, Object>) gson.fromJson(json, HashMap.class);
    assertEquals("my string", decoded.get("aString"));
  }

  @Test
  public void testPrefixKey() {
    Bytes serialized = serde.key(ORIGINAL);
    Bytes prefix4Keys = serde.prefixKeySerde(4).key(ORIGINAL);

    assertEquals(4, serde.numFields());
    assertEquals(4, serde.prefixKeySerde(4).numFields());
    assertEquals(serialized, prefix4Keys);

    //This one will be missing the last enum
    Bytes prefix3Keys = serde.prefixKeySerde(3).key(ORIGINAL);
    assertEquals(serialized.get().length - OrderedBytes.FIXED_INT_16_SIZE, prefix3Keys.get().length);
    assertTrue(OrderedBytes.startsWith(serialized.get(), prefix3Keys.get()));
    assertArrayEquals(Arrays.copyOfRange(serialized.get(), 0, prefix3Keys.get().length), prefix3Keys.get());

    //This one will be missing the last enum and the long
    Bytes prefix2Keys = serde.prefixKeySerde(2).key(ORIGINAL);
    assertEquals(serialized.get().length - OrderedBytes.FIXED_INT_16_SIZE - OrderedBytes.FIXED_INT_64_SIZE, prefix2Keys.get().length);
    assertTrue(OrderedBytes.startsWith(serialized.get(), prefix2Keys.get()));
    assertTrue(prefix3Keys.get().length > prefix2Keys.get().length);
  }

}
