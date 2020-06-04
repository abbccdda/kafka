/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import static io.confluent.telemetry.events.EventLoggerConfig.CLOUD_EVENT_BINARY_ENCODING;
import static io.confluent.telemetry.events.EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.telemetry.events.Test.AMessage;
import io.confluent.telemetry.events.Test.AnEnum;
import io.confluent.telemetry.events.Event;
import io.confluent.telemetry.events.Event.Builder;
import java.time.ZonedDateTime;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class ProtobufSerdeTest {

  private final AMessage entry = AMessage.newBuilder()
      .setALong(1029309)
      .setAString("my string")
      .setAnEnum(AnEnum.A)
      .setSecondString("second string")
      .build();

  Builder<AMessage> aMessageBuilder = Event.<AMessage>newBuilder()
      .setType("AMessage")
      .setSource("crn://authority/serde=source")
      .setSubject("crn://authority/serde=subject")
      .setId("26e0a193-7c6e-4915-b6ee-41559d980d65")
      .setTime(ZonedDateTime.parse("2020-05-09T01:02:12.031071Z"))
      .setData(entry);

  String jsonAMessageEvent = "{\"data\":{\"anenum\":\"A\",\"astring\":\"my string\",\"along\":1029309,\"anint\":0,\"anotherstring\":\"\",\"secondString\":\"second string\"},\"id\":\"26e0a193-7c6e-4915-b6ee-41559d980d65\",\"source\":\"crn://authority/serde=source\",\"specversion\":\"1.0\",\"type\":\"AMessage\",\"time\":\"2020-05-09T01:02:12.031Z\",\"datacontenttype\":\"application/json\",\"subject\":\"crn://authority/serde=subject\"}";

  public static Map expectedHeaders = ImmutableMap.of(
      "ce_specversion", "1.0",
      "ce_time", "2020-05-09T01:02:12.031071Z",
      "ce_subject", "crn://authority/serde=subject",
      "ce_source", "crn://authority/serde=source",
      "ce_id", "26e0a193-7c6e-4915-b6ee-41559d980d65");

  @Test
  public void roundtripStructured() {
    roundtrip(Protobuf.structuredSerializer(), Protobuf.structuredDeserializer(AMessage.class),
        CLOUD_EVENT_STRUCTURED_ENCODING);
  }

  @Test
  public void roundtripBinary() {
    roundtrip(Protobuf.binarySerializer(),
        Protobuf.binaryDeserializer(AMessage.parser()),
        CLOUD_EVENT_BINARY_ENCODING);
  }

  public void roundtrip(Serializer<AMessage> sf, Deserializer<AMessage> d, String enc) {

    CloudEvent<AttributesImpl, AMessage> event = aMessageBuilder
        .setDataContentType(Protobuf.contentType(enc))
        .build();
    ProducerRecord<String, byte[]> e = sf.producerRecord(event, "foo", 1);

    CloudEvent<AttributesImpl, AMessage> expected = d.deserialize(e.headers(), e.value());

    assertThat(expected.getAttributes().getType()).isEqualTo("AMessage");
    assertThat(expected.getData().get().getALong()).isEqualTo(1029309);
  }

  @Test
  public void serializeStructured() {
    Serializer<AMessage> sf = Protobuf.structuredSerializer();
    CloudEvent<AttributesImpl, AMessage> event = aMessageBuilder
        .setDataContentType(Protobuf.contentType(CLOUD_EVENT_STRUCTURED_ENCODING))
        .build();
    ProducerRecord<String, byte[]> e = sf.producerRecord(event, "foo", 1);
    assertThatJson(new String(e.value())).isEqualTo(jsonAMessageEvent);
  }

  @Test
  public void deserializeStructured() {
    Deserializer<AMessage> d = Protobuf.structuredDeserializer(AMessage.class);
    CloudEvent<AttributesImpl, AMessage> expected = d
        .deserialize(new RecordHeaders(), jsonAMessageEvent.getBytes());

    assertThat(expected.getData().get().getALong()).isEqualTo(1029309);
    assertThat(expected.getAttributes().getType()).isEqualTo("AMessage");
  }

  @Test
  public void serializeBinary() {
    Serializer<AMessage> sf = Protobuf.binarySerializer();
    CloudEvent<AttributesImpl, AMessage> event = aMessageBuilder
        .setDataContentType(Protobuf.contentType(CLOUD_EVENT_BINARY_ENCODING))
        .build();
    ProducerRecord<String, byte[]> e = sf.producerRecord(event, "AMessage", 1);
    assertThat(e.value()).isEqualTo(entry.toByteArray());
    assertThat(new String(e.headers().headers("content-type").iterator().next().value()))
        .isEqualTo(Protobuf.APPLICATION_PROTOBUF);
    checkHeader(e.headers(), "ce_specversion");
    checkHeader(e.headers(), "ce_subject");
    checkHeader(e.headers(), "ce_source");
    checkHeader(e.headers(), "ce_id");
    checkHeader(e.headers(), "ce_time");
  }

  public static void checkHeader(Headers headers, String key) {
    String h = new String(headers.headers(key).iterator().next().value());
    assertThat(h).isEqualTo(expectedHeaders.get(key));
  }

  @Test
  public void deserializeBinary() {
    Headers h = new RecordHeaders();
    h.add("ce_specversion", "1.0".getBytes());
    h.add("ce_time", "2020-05-09T00:56:14.471123Z".getBytes());
    h.add("ce_subject", "crn://authority/serde=subject".getBytes());
    h.add("content-type", Protobuf.APPLICATION_PROTOBUF.getBytes());
    h.add("ce_source", "crn://authority/serde=source".getBytes());
    h.add("ce_id", "ebfa4f0d-6a5e-4839-b3f6-ca4250a9332c".getBytes());
    h.add("ce_type", "AMessage".getBytes());

    Deserializer<AMessage> d = Protobuf.binaryDeserializer(AMessage.parser());
    CloudEvent<AttributesImpl, AMessage> expected = d.deserialize(h, entry.toByteArray());

    assertThat(expected.getData().get().getALong()).isEqualTo(1029309);
    assertThat(expected.getAttributes().getType()).isEqualTo("AMessage");
  }

}