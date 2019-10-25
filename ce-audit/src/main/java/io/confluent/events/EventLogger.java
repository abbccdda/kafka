/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events;

import io.cloudevents.CloudEvent;
import io.confluent.events.exporter.Exporter;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

/**
 * This creates a logger that writes events to the configured exporter. This enforces that for any
 * given logger name, there is a single exporter, to allow us to be sure that we have only one Kafka
 * Producer for each event log.
 */
public class EventLogger implements Reconfigurable, AutoCloseable {

  private Exporter exporter;
  private boolean configured = false;

  public EventLogger() {
  }

  /**
   * The log method synchronously waits for the topic to be created if it does not exist yet. If
   * topic creation fails, the event is logged to Kafka exporter's log4j logger along with the
   * error. If this behavior is undesirable, then you should ensure the topics to be created are
   * added to the configuration and check if the routes are ready or configure the topic creation to
   * be non-blocking using `event.logger.blocking=false`. Note that a RuntimeException is thrown if
   * the topic is not exists if the call is non-blocking.
   */

  public void log(CloudEvent event) {
    if (!this.configured) {
      throw new RuntimeException("EventLogger is not configured yet !");
    }
    this.exporter.append(event);
  }

  /**
   * Is the event exporter ready for sending events ? This method does not block. Calling this
   * method will trigger a reconciliation of routes in the background. For Kafka, this means the
   * topic manager will run its reconcile
   *
   * This is required for making sure the producer and topics are ready before sending a high volume
   * data stream. This is mainly needed for bootstrapping the audit log provider. The provider can
   * use this method to verify that the Kafka exporter is ready before sending events.
   */
  public boolean ready(CloudEvent event) {
    // If the logger is not configured yet, return false.
    if (!this.configured) {
      return false;
    }
    return this.exporter.routeReady(event);
  }

  // Handle dynamic reconfiguration. This is mainly to support UI config usecases for Audit events.
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
  public Exporter eventExporter() {
    return exporter;
  }
}
