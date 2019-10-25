/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events.cloudevents.kafka;

import static io.cloudevents.extensions.DistributedTracingExtension.Format.IN_MEMORY_KEY;
import static io.cloudevents.extensions.DistributedTracingExtension.Format.TRACE_PARENT_KEY;
import static io.cloudevents.extensions.DistributedTracingExtension.Format.TRACE_STATE_KEY;
import static io.cloudevents.json.Json.MAPPER;
import static java.util.Optional.ofNullable;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.cloudevents.Attributes;
import io.cloudevents.extensions.DistributedTracingExtension;
import io.cloudevents.extensions.DistributedTracingExtension.Format;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.format.BinaryUnmarshaller;
import io.cloudevents.format.StructuredUnmarshaller;
import io.cloudevents.format.builder.HeadersStep;
import io.cloudevents.fun.DataUnmarshaller;
import io.cloudevents.json.Json;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.CloudEventImpl;
import io.cloudevents.v03.kafka.AttributeMapper;
import io.cloudevents.v03.kafka.ExtensionMapper;
import io.confluent.events.cloudevents.extensions.RouteExtension;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.SerializationException;

public class Unmarshallers {

  private Unmarshallers() {
  }

  public static <T extends Message, A extends Attributes> DataUnmarshaller<byte[], T, A>
  binaryUmarshaller(T instance) {

    return new DataUnmarshaller<byte[], T, A>() {
      @Override
      public T unmarshal(byte[] data, A attributes) {
        if (data == null) {
          return null;
        }
        try {
          return ((Parser<T>) instance.getParserForType()).parseFrom(data);
        } catch (SerializationException e) {
          throw e;
        } catch (Exception e) {
          String errMsg = "Error deserializing protobuf message";
          throw new SerializationException(errMsg, e);
        }
      }
    };
  }

  public static <T extends Message> HeadersStep<AttributesImpl, T, byte[]> binaryProto(T instance) {

    Class<T> type = (Class<T>) instance.getClass();
    return BinaryUnmarshaller.<AttributesImpl, T, byte[]>
        builder()
        .map(AttributeMapper::map)
        .map(AttributesImpl::unmarshal)
        .map("application/json", Json.binaryUmarshaller(type))
        .map("application/protobuf", binaryUmarshaller(instance))
        .next()
        .map(ExtensionMapper::map)
        .map(DistributedTracingExtension::unmarshall)
        .map(RouteExtension::unmarshall)
        .next()
        .builder(CloudEventBuilder.<T>builder()::build);

  }

  /**
   * Builds a Structured Content Mode unmarshaller to unmarshal JSON as CloudEvents data for Kafka
   * Transport Binding
   *
   * @param <T> The 'data' type
   * @param typeOfData The type reference to use for 'data' unmarshal
   * @return A step to supply the headers, payload and to unmarshal
   * @see StructuredUnmarshaller
   */
  @SuppressWarnings("unchecked")
  public static <T> HeadersStep<AttributesImpl, T, byte[]> structuredProto(Class<T> typeOfData) {

    MAPPER.registerModule(new ProtobufModule());
    return StructuredUnmarshaller.<AttributesImpl, T, byte[]>
        builder()
        .map(ExtensionMapper::map)
        .map(DistributedTracingExtension::unmarshall)
        .map(RouteExtension::unmarshall)
        .next()
        .map((payload, extensions) -> {
          CloudEventImpl<T> event = Json
              .binaryDecodeValue(payload, CloudEventImpl.class, typeOfData);

          Optional<ExtensionFormat> dteFormat = ofNullable(event.getExtensions().get(IN_MEMORY_KEY))
              .filter(extension -> extension instanceof Map)
              .map(extension -> (Map<String, Object>) extension)
              .map(extension ->
                  extension.entrySet()
                      .stream()
                      .filter(entry ->
                          null != entry.getKey()
                              && null != entry.getValue())
                      .map(tracing ->
                          new SimpleEntry<>(tracing.getKey(),
                              tracing.getValue().toString()))
                      .collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
              .map(extension -> {
                DistributedTracingExtension dte = new DistributedTracingExtension();
                dte.setTraceparent(extension.get(TRACE_PARENT_KEY));
                dte.setTracestate(extension.get(TRACE_STATE_KEY));

                return new Format(dte);
              });

          Optional<ExtensionFormat> routingFormat =
              ofNullable(event.getExtensions().get(RouteExtension.Format.IN_MEMORY_KEY))
                  .filter(extension -> extension instanceof Map)
                  .map(extension -> (Map<String, Object>) extension)
                  .map(extension ->
                      extension.entrySet()
                          .stream()
                          .filter(entry ->
                              null != entry.getKey()
                                  && null != entry.getValue())
                          .map(tracing ->
                              new SimpleEntry<>(tracing.getKey(),
                                  tracing.getValue().toString()))
                          .collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
                  .map(extension -> {
                    RouteExtension re = new RouteExtension();
                    re.setRoute(extension.get(RouteExtension.Format.ROUTE_KEY));
                    return new RouteExtension.Format(re);
                  });

          CloudEventBuilder<T> builder =
              CloudEventBuilder.builder(event);

          extensions.get().forEach(extension -> {
            builder.withExtension(extension);
          });

          dteFormat.ifPresent(tracing -> {
            builder.withExtension(tracing);
          });

          routingFormat.ifPresent(route -> {
            builder.withExtension(route);
          });

          return builder.build();
        });
  }
}
