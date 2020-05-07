/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import java.util.Map;
import java.util.Optional;

/**
 * A provider for {@link KafkaHttpServlet}.
 *
 * <p>{@link KafkaHttpServlet KafkaHttpServlets} that wish to be loaded by the {@link
 * KafkaHttpServer} through the means described in {@link KafkaHttpServletLoader} should implement
 * this interface.
 *
 * @see KafkaHttpServletLoader#load(Map, KafkaHttpServerInjector) for details
 *
 * @since 6.0.0
 */
public interface KafkaHttpServletProvider {

  /**
   * Returns an instance of {@link KafkaHttpServlet}, or {@link Optional#empty()} if the application
   * is disabled.
   *
   * @since 6.0.0
   */
  Optional<KafkaHttpServlet> provide(
      Map<String, Object> configuration, KafkaHttpServerInjector injector);
}
