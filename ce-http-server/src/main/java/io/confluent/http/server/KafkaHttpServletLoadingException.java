package io.confluent.http.server;

/**
 * Exception thrown when the Kafka HTTP server fails to load a servlet.
 */
public final class KafkaHttpServletLoadingException extends RuntimeException {

  KafkaHttpServletLoadingException(String message, Throwable cause) {
    super(message, cause);
  }
}
