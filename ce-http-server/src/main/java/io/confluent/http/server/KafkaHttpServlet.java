/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * An HTTP Servlet to run an application in the {@link KafkaHttpServer}.
 */
@Deprecated
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
    configurePreResourceHandling(context, null);
  }

  /**
   * Configure your JAX-RS resource handling components which require access to
   * the sslContextFactory. i.e. Transparent forwarding of requests.
   */
  default void configurePreResourceHandling(ServletContextHandler context,
                                            SslContextFactory sslContextFactory) {
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

  /**
   * Shutdown hook that is invoked after the server has processed the shutdown request,
   * stopped accepting new connections, and tried to gracefully finish existing requests. At this
   * point it should be safe to clean up any resources used while processing requests.
   */
  public void onShutdown();

}
