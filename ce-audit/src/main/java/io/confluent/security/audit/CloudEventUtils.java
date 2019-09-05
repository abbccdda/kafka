package io.confluent.security.audit;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.time.Instant;
import java.util.UUID;

public class CloudEventUtils {

  public static final String CLOUDEVENTS_SPEC_VERSION = "0.3";
  private static TypeRegistry dataTypeRegistry;

  static {
    // Do we need to allow runtime registration?
    dataTypeRegistry = TypeRegistry.newBuilder()
        .add(AuditLogEntry.getDescriptor())
        .build();
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
        .setData(Any.pack(data))
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

}
