/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events.cloudevents.kafka;

import static io.cloudevents.json.Json.MAPPER;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.format.BinaryMarshaller;
import io.cloudevents.format.StructuredMarshaller;
import io.cloudevents.format.Wire;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.json.Json;
import io.cloudevents.v03.Accessor;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventImpl;
import io.cloudevents.v03.kafka.HeaderMapper;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Marshallers {

  private static final Map<String, byte[]> NO_HEADERS = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(Marshallers.class);

  static {
    // Ensure module is registered. This ensures the mapper is initialized completely
    // before main(...) starts up. See: https://confluentinc.atlassian.net/browse/CPKAFKA-3888
    ObjectMapper o = MAPPER.registerModule(new ProtobufModule());
    log.info("Registered Jackson modules {} ", o.getRegisteredModuleIds());
  }

  private Marshallers() {
  }

  /**
   * Builds a Binary Content Mode marshaller to marshal cloud events as JSON for Kafka Transport
   * Binding
   *
   * @param <T> The 'data' type
   * @return A step to provide the {@link CloudEventImpl} and marshal as JSON
   * @see BinaryMarshaller
   */
  public static <T extends Message> EventStep<AttributesImpl, T, byte[], byte[]> binaryProto() {

    return BinaryMarshaller.<AttributesImpl, T, byte[], byte[]>
        builder()
        .map(AttributesImpl::marshal)
        .map(Accessor::extensionsOf)
        .map(ExtensionFormat::marshal)
        .map(HeaderMapper::map)
        .map((data, headers) -> data.toByteArray())
        .builder(Wire::new);
  }

  /**
   * Builds a Structured Content Mode marshaller to marshal cloud event as JSON for Kafka Transport
   * Binding
   *
   * @param <T> The 'data' type
   * @return A step to provider the {@link CloudEventImpl} and marshal as JSON
   * @see StructuredMarshaller
   */
  public static <T extends Message> EventStep<AttributesImpl, T, byte[], byte[]> structuredProto() {

    return StructuredMarshaller.<AttributesImpl, T, byte[], byte[]>
        builder()
        .mime("content-type", "application/cloudevents+json".getBytes())
        .map(event -> Json.binaryMarshal(event, NO_HEADERS))
        .skip();
  }

}
