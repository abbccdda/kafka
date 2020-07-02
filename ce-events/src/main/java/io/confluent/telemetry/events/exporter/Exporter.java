/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.exporter;

import org.apache.kafka.common.Reconfigurable;

/**
 * Exporters are responsible for delivering events to their destinations
 */

public interface Exporter<T> extends Reconfigurable, AutoCloseable {

  /**
   * Filter and transform the events as appropriate and send to the specified destination. This
   * method takes care of batching, serialization and retries.
   */
  void emit(T event) throws RuntimeException;

  /**
   * Checks if a topic is ready. This is specifically used for transports where the routes require
   * some time to be ready. For eg. Kafka topics. For high volume audit event streams, the events
   * are only sent to Kafka exporter if the topic is ready.
   *
   * @return if the route for this event is ready.
   */
  default boolean routeReady(T event) {
    return true;
  }

}

