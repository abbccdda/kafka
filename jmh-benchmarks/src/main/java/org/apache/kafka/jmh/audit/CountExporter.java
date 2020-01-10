package org.apache.kafka.jmh.audit;

import static io.confluent.events.cloudevents.kafka.Marshallers.structuredProto;

import io.cloudevents.CloudEvent;
import io.cloudevents.format.Wire;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.events.cloudevents.extensions.RouteExtension;
import io.confluent.events.exporter.Exporter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

public class CountExporter implements Exporter {

  public RuntimeException configureException;
  public boolean routeReady = true;
  public ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();
  private EventStep<AttributesImpl, ? extends Object, byte[], byte[]> builder;

  public CountExporter() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configureException != null) {
      throw configureException;
    }
    this.builder = structuredProto();
  }

  private String route(CloudEvent event) {
    if (event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY)) {
      RouteExtension re = (RouteExtension) event.getExtensions()
          .get(RouteExtension.Format.IN_MEMORY_KEY);
      if (!re.getRoute().isEmpty()) {
        return re.getRoute();
      }
    }
    return "default";
  }

  @SuppressWarnings("unchecked")
  @Override
  public void append(CloudEvent event) throws RuntimeException {
    // A default topic should have matched, even if no explicit routing is configured
    String topicName = route(event);

    counts.compute(topicName, (k, v) -> v == null ? 1 : v + 1);

    ProducerRecord<String, byte[]> result = marshal(event, builder, topicName, null);

  }


  // The following code is copied from the Cloudevents SDK as the cloudevent producer wraps an older producer interface.
  @SuppressWarnings("unchecked")
  private <T> Wire<byte[], String, byte[]> marshal(Supplier<CloudEvent<AttributesImpl, T>> event,
      EventStep<AttributesImpl, T, byte[], byte[]> builder) {

    return Optional.ofNullable(builder)
        .map(step -> step.withEvent(event))
        .map(marshaller -> marshaller.marshal())
        .get();

  }

  private Set<Header> marshal(Map<String, byte[]> headers) {

    return headers.entrySet()
        .stream()
        .map(header -> new RecordHeader(header.getKey(), header.getValue()))
        .collect(Collectors.toSet());

  }

  private <T> ProducerRecord<String, byte[]> marshal(CloudEvent<AttributesImpl, T> event,
      EventStep<AttributesImpl, T, byte[], byte[]> builder,
      String topic,
      Integer partition) {
    Wire<byte[], String, byte[]> wire = marshal(() -> event, builder);
    Set<Header> headers = marshal(wire.getHeaders());

    Long timestamp = null;
    if (event.getAttributes().getTime().isPresent()) {
      timestamp = event.getAttributes().getTime().get().toInstant().toEpochMilli();
    }

    if (!wire.getPayload().isPresent()) {
      throw new RuntimeException("payload is empty");
    }

    byte[] payload = wire
        .getPayload()
        .get();

    return new ProducerRecord<>(
        topic,
        partition,
        timestamp,
        // Get partitionKey from cloudevent extensions once it is supported upstream.
        null,
        payload,
        headers);
  }


  @Override
  public boolean routeReady(CloudEvent event) {
    return routeReady;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Collections.emptySet();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
  }

  @Override
  public void close() throws Exception {
  }
}
