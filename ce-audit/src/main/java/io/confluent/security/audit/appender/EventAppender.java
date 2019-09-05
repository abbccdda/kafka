package io.confluent.security.audit.appender;

import io.confluent.security.audit.CloudEvent;
import org.apache.kafka.common.Reconfigurable;

/**
 * EventAppenders are responsible for delivering events to their destinations
 */
public interface EventAppender extends Reconfigurable, AutoCloseable {

  /**
   * Filter and transform the events as appropriate and send to the specified destination
   */
  void append(CloudEvent event) throws RuntimeException;
}
