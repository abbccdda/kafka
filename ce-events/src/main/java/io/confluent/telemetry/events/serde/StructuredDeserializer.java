/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import static io.cloudevents.extensions.DistributedTracingExtension.Format.IN_MEMORY_KEY;
import static io.cloudevents.extensions.DistributedTracingExtension.Format.TRACE_PARENT_KEY;
import static io.cloudevents.extensions.DistributedTracingExtension.Format.TRACE_STATE_KEY;
import static java.util.Optional.ofNullable;

import io.cloudevents.extensions.DistributedTracingExtension;
import io.cloudevents.extensions.DistributedTracingExtension.Format;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.format.StructuredUnmarshaller;
import io.cloudevents.format.builder.HeadersStep;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.CloudEventImpl;
import io.cloudevents.v03.kafka.ExtensionMapper;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class that deserializes a cloudevent from a Kafka message.
 *
 * Note that this does not implement the Kafka Deserializer interface because we need the entire
 * ConsumerRecord to decode the cloudevent message properly.
 */

public class StructuredDeserializer<T> extends Deserializer<T> {

  public StructuredDeserializer(Function<byte[], CloudEventImpl<T>> structuredUnmarshaller) {
    this.builder = structuredBuilder(structuredUnmarshaller);

  }

  /**
   * The default Kafka UnMarshallers do not provide hooks to customize the unmarshalling of the
   * cloudevent payload and custom extensions. See https://github.com/cloudevents/sdk-java/blob/v0.3.1/api/README.md#unmarshaller
   * for details on step builder pattern used below.
   */
  @SuppressWarnings("unchecked")
  protected HeadersStep<AttributesImpl, T, byte[]> structuredBuilder(
      Function<byte[], CloudEventImpl<T>> structuredUnmarshaller
  ) {
    /*
     * Step 0. Define the types - there are three
     *   - Type 1 -> AttributesImpl: the implementation of attributes
     *   - Type 2 -> T..........: the type CloudEvents' 'data'
     *   - Type 3 -> byte[]........: the type of payload used in the unmarshalling
     */

    return StructuredUnmarshaller.<AttributesImpl, T, byte[]>
        builder()
        /*
         * Step 1. The extension mapping -  map transport headers to map of extensions
         */
        .map(ExtensionMapper::map)
        /*
         * Step 2. The extension unmarshaller
         *   - we must provide an impl able to unmarshal from map of extenstions into actual ones
         */
        .map(DistributedTracingExtension::unmarshall)
        .map(RouteExtension::unmarshall)
        .next()
        /*
         * Step 3. Envelope unmarshaller
         *   - we must provide an impl able to unmarshal the envelope into cloudevents
         *   - now we get the HeadersStep<AttributesImpl, Much, String>, a common step that event unmarshaller must returns
         *   - from here we just call withHeaders(), withPayload() and unmarshal()
         */
        .map((payload, extensions) -> {

          CloudEventImpl<T> event;

          event = structuredUnmarshaller.apply(payload);

          /*
           * Set the extensions on the cloud event.
           */
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

          CloudEventBuilder<T> builder = CloudEventBuilder.builder(event);
          extensions.get().forEach(builder::withExtension);
          dteFormat.ifPresent(builder::withExtension);
          routingFormat.ifPresent(builder::withExtension);
          return builder.build();
        });
  }

}
