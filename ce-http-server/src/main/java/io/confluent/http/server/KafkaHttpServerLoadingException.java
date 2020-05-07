package io.confluent.http.server;

/**
 * Exception thrown when the Kafka HTTP server fails to load a servlet.
 */
public final class KafkaHttpServerLoadingException extends RuntimeException {

  KafkaHttpServerLoadingException(String message, Throwable cause) {
    super(message, cause);
  }
}
