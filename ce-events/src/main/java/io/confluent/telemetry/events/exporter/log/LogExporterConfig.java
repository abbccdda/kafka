/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events.exporter.log;

import static org.apache.kafka.common.config.internals.ConfluentConfigs.EVENT_LOGGER_PREFIX;

import java.util.Map;
import io.confluent.telemetry.events.exporter.ExporterConfig;
import org.apache.kafka.common.config.ConfigDef;

public class LogExporterConfig extends ExporterConfig {

  // Configuration for LogExporter
  public static final String LOG_EVENT_EXPORTER_NAME_CONFIG = EVENT_LOGGER_PREFIX + "exporter.log.name";
  public static final String DEFAULT_LOG_EVENT_EXPORTER_NAME_CONFIG = "event";
  public static final String LOG_EVENT_EXPORTER_NAME_DOC = "Name for the logger, used in getLogger()";
  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(
            LOG_EVENT_EXPORTER_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_LOG_EVENT_EXPORTER_NAME_CONFIG,
            ConfigDef.Importance.LOW,
            LOG_EVENT_EXPORTER_NAME_DOC
        );
  }

  public LogExporterConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }


}
