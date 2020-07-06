/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A provider for {@link KafkaHttpServer}.
 *
 * <p>A {@code KafkaHttpServer} implementation that wishes to be loaded by the Kafka broker through
 * the means described in {@link KafkaHttpServerLoader} should implement this interface.
 *
 * @see KafkaHttpServerLoader#load(Map, KafkaHttpServerInjector)
 *
 * @since 6.0.0
 */
public interface KafkaHttpServerProvider {

  /**
   * Returns a new instance of {@link KafkaHttpServer}, or {@link Optional#empty()} if this
   * implementation is disabled.
   *
   * <p>If this method returns a non-empty value, it should always be a <b>NEW</b> instance of
   * {@code KafkaHttpServer}.
   *
   * @since 6.0.0
   */
  @SuppressWarnings("deprecation")
  default Optional<KafkaHttpServer> provide(
      Map<String, Object> configuration, KafkaHttpServerInjector injector) {
    List<KafkaHttpServlet> servlets = KafkaHttpServletLoader.load(configuration, injector);
    if (servlets.isEmpty()) {
      return Optional.empty();
    }
    return provide(configuration, servlets);
  }

  /**
   * Returns a new instance of {@link KafkaHttpServer}, or {@link Optional#empty()} if this
   * implementation is disabled.
   *
   * <p>If this method returns a non-empty value, it should always be a <b>NEW</b> instance of
   * {@code KafkaHttpServer}.
   *
   * @since 6.0.0
   */
  @Deprecated
  default Optional<KafkaHttpServer> provide(
      Map<String, Object> configuration, List<KafkaHttpServlet> servlets) {
    throw new UnsupportedOperationException("To be deleted.");
  }
}
