/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.http.server;

import java.lang.annotation.Annotation;
import java.util.function.Supplier;

/**
 * An injector that can be used by {@link KafkaHttpServerLoader} to inject runtime dependencies from
 * the Kafka broker to {@link KafkaHttpServer}.
 *
 * @since 6.0.0
 */
public interface KafkaHttpServerInjector {

  /**
   * Returns the instance (or null) configured for {@code type} in this injector, if such binding
   * exists. Throws {@link ProvisionException} otherwise.
   *
   * <p>Calling this method is equivalent to calling {@link #getInstance(Class, Class)
   * getInstance(type, null)}.
   *
   * @since 6.0.0
   */
  <T> T getInstance(Class<T> type) throws ProvisionException;

  /**
   * Returns the instance (or null) configured for {@code type}, annotated with {@code annotation},
   * in this injector, if such binding exists. Throws {@link ProvisionException} otherwise.
   *
   * @since 6.0.0
   */
  <T> T getInstance(Class<T> type, Class<? extends Annotation> annotation)
      throws ProvisionException;

  /**
   * Returns the supplier configured for {@code type} in this injector, if such binding exists.
   * Throws {@link ProvisionException} otherwise.
   *
   * <p>Calling this method is equivalent to calling {@link #getSupplier(Class, Class)
   * getSupplier(type, null)}.
   *
   * @since 6.0.0
   */
  <T> Supplier<T> getSupplier(Class<T> type) throws ProvisionException;

  /**
   * Returns the supplier configured for {@code type}, annotated with {@code annotation}, in this
   * injector, if such binding exists. Throws {@link ProvisionException} otherwise.
   *
   * @since 6.0.0
   */
  <T> Supplier<T> getSupplier(Class<T> type, Class<? extends Annotation> annotation)
      throws ProvisionException;

  /**
   * Exception thrown when {@link KafkaHttpServerInjector} fails to provide for a requested binding.
   *
   * @since 6.0.0
   */
  final class ProvisionException extends RuntimeException {

    /**
     * See {@link RuntimeException#RuntimeException()}.
     *
     * @since 6.0.0
     */
    public ProvisionException() {
      super();
    }

    /**
     * See {@link RuntimeException#RuntimeException(String)}.
     *
     * @since 6.0.0
     */
    public ProvisionException(String message) {
      super(message);
    }

    /**
     * See {@link RuntimeException#RuntimeException(String, Throwable)}.
     *
     * @since 6.0.0
     */
    public ProvisionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
