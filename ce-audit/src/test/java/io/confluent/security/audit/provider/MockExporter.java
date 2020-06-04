/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.telemetry.events.exporter.Exporter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;

public class MockExporter<T> implements Exporter<T> {

  public RuntimeException configureException;
  public boolean routeReady = true;
  public final ArrayList<CloudEvent<AttributesImpl, T>> events = new ArrayList<>();

  public MockExporter() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configureException != null) {
      throw configureException;
    }
  }

  @Override
  public void append(CloudEvent<AttributesImpl, T> event) throws RuntimeException {
    events.add(event);
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

  public void setRouteReady(boolean ready) {
    routeReady = ready;
  }
}
