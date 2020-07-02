/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events;

import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.telemetry.events.exporter.Exporter;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

/**
 * This creates a logger that writes events to the configured exporter.
 */
public class EventLogger<T> implements Reconfigurable, AutoCloseable {

  private Exporter<CloudEvent<AttributesImpl, T>> exporter;
  private boolean configured = false;

  public EventLogger() {
  }

  /**
   * Log an event.
   * This method might block if the exporter is not ready yet. If you need non-blocking
   * behavior then you should use the ready(..) method to ensure the exporter is ready
   * before sending events.
   * @param event
   */

  public void log(CloudEvent<AttributesImpl, T> event) {
    if (!this.configured) {
      throw new IllegalStateException("EventLogger is not configured yet !");
    }
    this.exporter.emit(event);
  }

  /**
   * Check if the event exporter ready for sending events. This method does not block.
   **
   * This is mainly needed for bootstrapping the audit log provider. The provider can
   * use this method to verify that the Kafka exporter is ready before sending events.
   */
  public boolean ready(CloudEvent<AttributesImpl, T>  event) {
    // If the logger is not configured yet, return false.
    if (!this.configured) {
      return false;
    }
    return this.exporter.routeReady(event);
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return this.exporter.reconfigurableConfigs();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    this.exporter.validateReconfiguration(configs);
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    this.exporter.reconfigure(configs);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    EventLoggerConfig config = new EventLoggerConfig(configs);
    // Create the exporter and configure it.
    this.exporter = config
        .getConfiguredInstance(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, Exporter.class);
    this.configured = true;
  }

  @Override
  public void close() throws Exception {
    if (this.exporter != null) {
      this.exporter.close();
    }
  }

  //For testing.
  public Exporter<CloudEvent<AttributesImpl, T>> eventExporter() {
    return exporter;
  }
}
