/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * An HTTP Servlet to run an application in the {@link KafkaHttpServer}.
 */
public interface KafkaHttpServlet {

  /**
   * Returns the path that this servlet's endpoints will be prefixed by.
   *
   * <p>This path should be unique across servlets.
   */
  String getPath();

  /**
   * Add any servlet filters that should be called before resource handling.
   */
  default void configurePreResourceHandling(ServletContextHandler context) {
  }

  /**
   * Configure your JAX-RS resource handling components.
   */
  default void configureResourceHandling(ResourceConfig context) {
  }

  /**
   * Add any servlet filters that should be called after resource handling.
   */
  default void configurePostResourceHandling(ServletContextHandler context) {
  }
}
