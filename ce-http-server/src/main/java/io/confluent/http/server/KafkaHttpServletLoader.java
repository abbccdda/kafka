/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static factory used to load {@link KafkaHttpServlet KafkaHttpServlets} from the classpath.
 */
final class KafkaHttpServletLoader {

  private static final Logger log = LoggerFactory.getLogger(KafkaHttpServletLoader.class);

  private KafkaHttpServletLoader() {
  }

  /**
   * Load and returns {@link KafkaHttpServlet KafkaHttpServlets} from this classpath.
   *
   * <p>The mechanism by which KafkaHttpServlets are found is JDK's 1.6 {@link ServiceLoader}. To
   * have your {@code KafkaHttpServlet} loaded, you need to implement {@link
   * KafkaHttpServletProvider} and register the implementation by the means described in {@code
   * ServiceLoader} documentation.
   *
   * <p>Each provider found will have the {@link KafkaHttpServletProvider#provide(Map,
   * KafkaHttpServerInjector)} method called with the given {@code configuration} and {@code
   * injector} as arguments.
   *
   * <p>If you want to prevent your {@code KafkaHttpServlet} from being loaded, make sure your
   * provider returns {@link Optional#empty()}.
   *
   * @throws KafkaHttpServletLoadingException if an error occurs when loading a servlet
   */
  static List<KafkaHttpServlet> load(
      Map<String, Object> configuration, KafkaHttpServerInjector injector) {
    ArrayList<KafkaHttpServlet> servlets = new ArrayList<>();
    for (KafkaHttpServletProvider provider : ServiceLoader.load(KafkaHttpServletProvider.class)) {
      Optional<KafkaHttpServlet> servlet;
      try {
        servlet = provider.provide(configuration, injector);
      } catch (Throwable error) {
        throw new KafkaHttpServletLoadingException(
            String.format(
                "Failed to load KafkaHttpServlet from provider %s.",
                provider.getClass().getSimpleName()),
            error);
      }
      if (!servlet.isPresent()) {
        log.info(
            "KafkaHttpServlet from provider {} is disabled. Skipping.",
            provider.getClass().getSimpleName());
        continue;
      }
      KafkaHttpServlet loaded = servlet.get();
      log.info("Loaded KafkaHttpServlet {}.", loaded.getClass());
      servlets.add(loaded);
    }
    return servlets;
  }
}
