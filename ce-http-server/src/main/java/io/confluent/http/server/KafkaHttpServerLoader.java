/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static factory used to load {@link KafkaHttpServer} from the classpath.
 *
 * <p><b>This class is for internal use of Confluent Server only. It should not be considered part
 * of its public interface. Changes to it can happen at any time without notice.</b>
 */
public final class KafkaHttpServerLoader {

  private static final Logger log = LoggerFactory.getLogger(KafkaHttpServerLoader.class);

  private KafkaHttpServerLoader() {
  }

  /**
   * Load and returns {@link KafkaHttpServer} from this classpath.
   *
   * <p>The mechanism by which implementations are found is JDK's 1.6 {@link ServiceLoader}. To have
   * your implementation loaded, you need to implement {@link KafkaHttpServerProvider} and register
   * the implementation by the means described in {@link ServiceLoader} documentation.
   *
   * <p>Each provider found will have the {@link KafkaHttpServerProvider#provide(Map,
   * java.util.List)} method called with the given {@code configuration} and {@code
   * injector} as arguments.
   *
   * <p>If you want to prevent your implementation from being loaded, make sure your provider
   * returns {@link Optional#empty()}.
   */
  public static Optional<KafkaHttpServer> load(
      Map<String, Object> configuration, KafkaHttpServerInjector injector) {
    Objects.requireNonNull(configuration);
    Objects.requireNonNull(injector);

    List<KafkaHttpServlet> applications = KafkaHttpServletLoader.load(configuration, injector);
    if (applications.isEmpty()) {
      log.info("No Kafka HTTP servlet configured. Not loading Kafka HTTP server.");
      return Optional.empty();
    }

    HashSet<KafkaHttpServer> implementations = new HashSet<>();
    for (KafkaHttpServerProvider provider : ServiceLoader.load(KafkaHttpServerProvider.class)) {
      Optional<KafkaHttpServer> implementation;
      try {
        implementation = provider.provide(configuration, applications);
      } catch (Throwable error) {
        throw new KafkaHttpServerLoadingException(
            String.format(
                "Failed to load KafkaHttpServer implementation from provider %s. Skipping.",
                provider.getClass().getSimpleName()),
            error);
      }
      if (!implementation.isPresent()) {
        log.info(
            "KafkaHttpServer implementation from provider {} is disabled. Skipping.",
            provider.getClass().getSimpleName());
        continue;
      }
      implementations.add(implementation.get());
    }

    if (implementations.isEmpty()) {
      log.info("No Kafka HTTP server implementation configured. Skipping.");
      return Optional.empty();
    }
    if (implementations.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "Multiple implementations found for KafkaHttpServer: %s.", implementations));
    }

    KafkaHttpServer implementation = implementations.iterator().next();
    log.info("Loaded KafkaHttpServer implementation {}", implementation.getClass());
    return Optional.of(implementation);
  }
}
