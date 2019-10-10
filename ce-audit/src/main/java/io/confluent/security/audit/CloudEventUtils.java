package io.confluent.security.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

public class CloudEventUtils {

  public static final String CLOUDEVENTS_SPEC_VERSION = "0.3";
  private static TypeRegistry dataTypeRegistry;
  public static final String TYPE_URL = "type.confluent.io";
  private static ObjectMapper objectMapper;

  static {
    // Do we need to allow runtime registration?
    dataTypeRegistry = TypeRegistry.newBuilder()
        .add(AuditLogEntry.getDescriptor())
        .build();
    objectMapper = new ObjectMapper();
  }

  /**
   * Put the supplied data into a CloudEvents envelope
   */
  public static CloudEvent wrap(String type, String source, String subject,
      String id, Instant time, Message data) {
    return CloudEvent.newBuilder()
        .setSpecversion(CLOUDEVENTS_SPEC_VERSION)
        .setType(type)
        .setSource(source)
        .setId(id)
        .setTime(time.toString())
        .setSubject(subject)
        .setData(Any.pack(data, TYPE_URL))
        .build();
  }

  /**
   * Put the supplied data into a CloudEvents envelope. Generate a random UUID for id and use now
   * for the time.
   */
  public static CloudEvent wrap(String type, String source, String subject, Message data) {
    return wrap(type, source, subject, UUID.randomUUID().toString(), Instant.now(), data);
  }

  /**
   * This allows the `data` types to be re-created from the Any
   */
  public static TypeRegistry dataTypeRegistry() {
    return dataTypeRegistry;
  }

  /**
   * Protobuf's JsonFormat uses the GSON library to produce a JSON string.
   * GSON does some character escaping by default that we don't want.
   * We use Jackson on this GSON-generated value to remove this escaping.
   */
  private static String toGsonString(CloudEvent event) throws InvalidProtocolBufferException {
    return JsonFormat.printer()
        .omittingInsignificantWhitespace()
        .includingDefaultValueFields()
        .usingTypeRegistry(dataTypeRegistry)
        .print(event);
  }

  /**
   * Return the JSON string for this CloudEvent
   */
  public static String toJsonString(CloudEvent event) throws IOException {
    return objectMapper
        .writeValueAsString(objectMapper.readTree(toGsonString(event)));
  }

  /**
   * Return the JSON string for this CloudEvent with whitespace for human readability
   */
  public static String toPrettyJsonString(CloudEvent event) throws IOException {
    return objectMapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(objectMapper.readTree(toGsonString(event)));
  }
}
