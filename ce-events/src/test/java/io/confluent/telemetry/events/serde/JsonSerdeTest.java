/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import static io.confluent.telemetry.events.serde.ProtobufSerdeTest.checkHeader;
import static org.assertj.core.api.Assertions.assertThat;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.telemetry.events.Event;
import java.time.ZonedDateTime;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class JsonSerdeTest {

  CloudEvent<AttributesImpl, String> strEvent = Event.<String>newBuilder()
      .setType("event_type")
      .setSource("crn://authority/serde=source")
      .setSubject("crn://authority/serde=subject")
      .setDataContentType(Json.APPLICATION_JSON)
      .setId("ebfa4f0d-6a5e-4839-b3f6-ca4250a9332c")
      .setTime(ZonedDateTime.parse("2020-05-09T00:56:14.471123Z"))
      .setData("foobar")
      .build();

  String jsonStringEvent = "{\"data\":\"foobar\",\"id\":\"ebfa4f0d-6a5e-4839-b3f6-ca4250a9332c\",\"source\":\"crn://authority/serde=source\",\"specversion\":\"0.3\",\"type\":\"event_type\",\"time\":\"2020-05-09T00:56:14.471123Z\",\"datacontenttype\":\"application/json\",\"subject\":\"crn://authority/serde=subject\"}";

  CloudEvent<AttributesImpl, TestMessage> testEvent = Event.<TestMessage>newBuilder()
      .setType("foo")
      .setSource("crn://authority/serde=source")
      .setSubject("crn://authority/serde=subject")
      .setDataContentType(Json.APPLICATION_JSON)
      .setId("26e0a193-7c6e-4915-b6ee-41559d980d65")
      .setTime(ZonedDateTime.parse("2020-05-09T01:02:12.031071Z"))
      .setData(TestMessage.INSTANCE)
      .build();

  String jsonTestEvent = "{\"data\":{\"a\":1,\"b\":\"bar\",\"c\":[\"F\",\"O\",\"O\"]},\"id\":\"26e0a193-7c6e-4915-b6ee-41559d980d65\",\"source\":\"crn://authority/serde=source\",\"specversion\":\"0.3\",\"type\":\"foo\",\"time\":\"2020-05-09T01:02:12.031071Z\",\"datacontenttype\":\"application/json\",\"subject\":\"crn://authority/serde=subject\"}";

  @Test
  public void roundtripStructured() {
    roundtripStr(Json.structuredSerializer(), Json.structuredDeserializer(String.class));
    roundtripObject(Json.structuredSerializer(), Json.structuredDeserializer(TestMessage.class));
  }

  @Test
  public void roundtripBinary() {
    roundtripStr(Json.binarySerializer(), Json.binaryDeserializer(String.class));
    roundtripObject(Json.binarySerializer(), Json.binaryDeserializer(TestMessage.class));

  }

  public void roundtripStr(Serializer<String> s, Deserializer<String> d) {
    ProducerRecord<String, byte[]> e = s.producerRecord(strEvent, "foo", 1);

    CloudEvent<AttributesImpl, String> expected = d.deserialize(e.headers(), e.value());

    assertThat(expected.getData().get()).isEqualTo("foobar");
    assertThat(expected.getAttributes().getType()).isEqualTo("event_type");
  }

  @Test
  public void serializeStringStructured() {
    Serializer<String> s = Json.<String>structuredSerializer();
    ProducerRecord<String, byte[]> e = s.producerRecord(strEvent, "footopic", 1);

    assertThat(new String(e.value())).isEqualTo(jsonStringEvent);
  }

  @Test
  public void deserializeStringStructured() {
    Deserializer<String> d = Json.structuredDeserializer(String.class);
    CloudEvent<AttributesImpl, String> expected = d
        .deserialize(new RecordHeaders(), jsonStringEvent.getBytes());

    assertThat(expected.getData().get()).isEqualTo("foobar");
    assertThat(expected.getAttributes().getType()).isEqualTo("event_type");
  }

  @Test
  public void serializeStringBinary() {
    Serializer<String> s = Json.binarySerializer();
    ProducerRecord<String, byte[]> e = s.producerRecord(strEvent, "foo", 1);
    assertThat(new String(e.value())).isEqualTo("\"foobar\"");
  }

  @Test
  public void deserializeStringBinary() {
    Headers h = new RecordHeaders();

    h.add("ce_specversion", "0.3".getBytes());
    h.add("ce_time", "2020-05-09T00:56:14.471123Z".getBytes());
    h.add("ce_subject", "crn://authority/serde=subject".getBytes());
    h.add("ce_datacontenttype", "application/json".getBytes());
    h.add("ce_source", "crn://authority/serde=source".getBytes());
    h.add("ce_id", "ebfa4f0d-6a5e-4839-b3f6-ca4250a9332c".getBytes());
    h.add("ce_type", "event_type".getBytes());

    Deserializer<String> d = Json.binaryDeserializer(String.class);
    CloudEvent<AttributesImpl, String> expected = d.deserialize(h, "\"foobaz\"".getBytes());

    assertThat(expected.getData().get()).isEqualTo("foobaz");
    assertThat(expected.getAttributes().getType()).isEqualTo("event_type");
  }

  public void roundtripObject(Serializer<TestMessage> sf, Deserializer<TestMessage> d) {
    ProducerRecord<String, byte[]> e = sf.producerRecord(testEvent, "foo", 1);

    CloudEvent<AttributesImpl, TestMessage> expected = d.deserialize(e.headers(), e.value());

    assertThat(expected.getData().get().getA()).isEqualTo(1);
    assertThat(expected.getAttributes().getType()).isEqualTo("foo");
  }

  @Test
  public void serializeObjectStructured() {
    Serializer<TestMessage> sf = Json.structuredSerializer();
    ProducerRecord<String, byte[]> e = sf.producerRecord(testEvent, "foo", 1);
    assertThat(new String(e.value())).isEqualTo(jsonTestEvent);
  }

  @Test
  public void deserializeObjectStructured() {
    Deserializer<TestMessage> d = Json.structuredDeserializer(TestMessage.class);
    CloudEvent<AttributesImpl, TestMessage> expected = d
        .deserialize(new RecordHeaders(), jsonTestEvent.getBytes());

    assertThat(expected.getData().get().getA()).isEqualTo(1);
    assertThat(expected.getAttributes().getType()).isEqualTo("foo");
  }

  @Test
  public void serializeObjectBinary() {
    Serializer<TestMessage> sf = Json.binarySerializer();
    ProducerRecord<String, byte[]> e = sf.producerRecord(testEvent, "foo", 1);
    assertThat(new String(e.value()))
        .isEqualTo("{\"a\":1,\"b\":\"bar\",\"c\":[\"F\",\"O\",\"O\"]}");
    assertThat(new String(e.headers().headers("ce_datacontenttype").iterator().next().value()))
        .isEqualTo(Protobuf.APPLICATION_JSON);
    checkHeader(e.headers(), "ce_specversion");
    checkHeader(e.headers(), "ce_subject");
    checkHeader(e.headers(), "ce_source");
    checkHeader(e.headers(), "ce_id");
    checkHeader(e.headers(), "ce_time");
  }

  @Test
  public void deserializeTestMessageBinary() {
    Headers h = new RecordHeaders();

    h.add("ce_specversion", "0.3".getBytes());
    h.add("ce_time", "2020-05-09T00:56:14.471123Z".getBytes());
    h.add("ce_subject", "crn://authority/serde=subject".getBytes());
    h.add("ce_datacontenttype", "application/json".getBytes());
    h.add("ce_source", "crn://authority/serde=source".getBytes());
    h.add("ce_id", "ebfa4f0d-6a5e-4839-b3f6-ca4250a9332c".getBytes());
    h.add("ce_type", "foo".getBytes());

    Deserializer<TestMessage> d = Json.binaryDeserializer(TestMessage.class);
    CloudEvent<AttributesImpl, TestMessage> expected = d
        .deserialize(h, "{\"a\":1,\"b\":\"bar\",\"c\":[\"F\",\"O\",\"O\"]}".getBytes());

    assertThat(expected.getData().get().getA()).isEqualTo(1);
    assertThat(expected.getAttributes().getType()).isEqualTo("foo");
  }
}
