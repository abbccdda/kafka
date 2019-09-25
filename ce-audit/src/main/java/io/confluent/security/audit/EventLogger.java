package io.confluent.security.audit;

import io.confluent.security.audit.appender.EventAppender;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

/**
 * This creates a logger that writes events to the configured appender. This enforces that for any
 * given logger name, there is a single appender, to allow us to be sure that we have only one Kafka
 * Producer for each event log.
 */
public class EventLogger implements Reconfigurable, AutoCloseable {

  private static ConcurrentHashMap<String, EventLogger> namedLoggers = new ConcurrentHashMap<>();
  private EventAppender eventAppender;

  private EventLogger() {
    // Managed by EventLogger.logger
  }

  public static EventLogger logger(String name, Map<String, ?> configs) {
    return namedLoggers.computeIfAbsent(name, k -> createLogger(configs));
  }

  public static EventLogger createLogger(Map<String, ?> configs) {
    EventLogConfig config = new EventLogConfig(configs);
    EventLogger logger = new EventLogger();
    logger.eventAppender = config
          .getConfiguredInstance(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG, EventAppender.class);
    return logger;
  }

  public void log(CloudEvent event) {
    eventAppender.append(event);
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return eventAppender.reconfigurableConfigs();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    eventAppender.validateReconfiguration(configs);
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    eventAppender.reconfigure(configs);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // since this can only be called on an instance that is already configured...
    reconfigure(configs);
  }

  public void close() throws Exception {
    if (eventAppender != null) {
      eventAppender.close();
    }
  }
}
