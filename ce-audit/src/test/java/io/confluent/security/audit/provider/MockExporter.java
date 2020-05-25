/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import io.cloudevents.CloudEvent;
import io.confluent.events.exporter.Exporter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;

public class MockExporter implements Exporter {

  public RuntimeException configureException;
  public boolean routeReady = true;
  public final ArrayList<CloudEvent> events = new ArrayList<>();

  public MockExporter() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configureException != null) {
      throw configureException;
    }
  }

  @Override
  public void append(CloudEvent event) throws RuntimeException {
    events.add(event);
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

  public void setRouteReady(boolean ready) {
    routeReady = ready;
  }
}
