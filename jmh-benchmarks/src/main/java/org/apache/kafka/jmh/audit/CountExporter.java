package org.apache.kafka.jmh.audit;

import com.google.protobuf.MessageLite;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;
import io.confluent.telemetry.events.serde.Serializer;
import io.confluent.telemetry.events.exporter.Exporter;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;

public class CountExporter<T extends MessageLite> implements Exporter<CloudEvent<AttributesImpl, T>> {

  public RuntimeException configureException;
  public boolean routeReady = true;
  public ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();
  private Serializer<T> serializer;

  public CountExporter() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configureException != null) {
      throw configureException;
    }
    this.serializer = Protobuf.structuredSerializer();
  }

  private String route(CloudEvent<AttributesImpl, T> event) {
    if (event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY)) {
      RouteExtension re = (RouteExtension) event.getExtensions()
          .get(RouteExtension.Format.IN_MEMORY_KEY);
      if (!re.getRoute().isEmpty()) {
        return re.getRoute();
      }
    }
    return "default";
  }

  @Override
  public void emit(CloudEvent<AttributesImpl, T> event) throws RuntimeException {
    // A default topic should have matched, even if no explicit routing is configured
    String topicName = route(event);

    counts.compute(topicName, (k, v) -> v == null ? 1 : v + 1);

    ProducerRecord<String, byte[]> result = serializer.producerRecord(event, topicName, null);

  }

  @Override
  public boolean routeReady(CloudEvent<AttributesImpl, T> event) {
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
