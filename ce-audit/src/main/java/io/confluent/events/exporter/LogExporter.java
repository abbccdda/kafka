/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events.exporter;

import io.cloudevents.CloudEvent;
import io.confluent.events.CloudEventUtils;
import io.confluent.events.EventLoggerConfig;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LogExporter sends events to the SLF4J-configured log file
 */

public class LogExporter implements Exporter {

  private Logger log = LoggerFactory.getLogger(LogExporter.class);

  @Override
  public void configure(Map<String, ?> configs) {
    EventLoggerConfig config = new EventLoggerConfig(configs);
    log = LoggerFactory
        .getLogger(config.getString(EventLoggerConfig.LOG_EVENT_EXPORTER_NAME_CONFIG));
  }

  @Override
  public void append(CloudEvent event) throws RuntimeException {
    log.info(CloudEventUtils.toJsonString(event));
  }

  @Override
  public boolean routeReady(CloudEvent event) {
    return true;
  }


  @Override
  public void close() throws Exception {

  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Utils.mkSet(EventLoggerConfig.LOG_EVENT_EXPORTER_NAME_CONFIG);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    configure(configs);
  }
}
