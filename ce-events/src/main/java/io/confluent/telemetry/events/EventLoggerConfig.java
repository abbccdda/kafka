/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events;

import static org.apache.kafka.common.config.internals.ConfluentConfigs.EVENT_LOGGER_PREFIX;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class EventLoggerConfig extends AbstractConfig {

  // Configuration for event exporters.
  public static final String EVENT_EXPORTER_CLASS_CONFIG = EVENT_LOGGER_PREFIX + "exporter.class";
  public static final String EVENT_EXPORTER_CLASS_DOC = "Class to use for delivering event logs.";

  public static final String CLOUD_EVENT_STRUCTURED_ENCODING = "structured";
  public static final String CLOUD_EVENT_BINARY_ENCODING = "binary";
  public static final String CLOUD_EVENT_ENCODING_CONFIG = EVENT_LOGGER_PREFIX + "cloudevent.codec";
  public static final String DEFAULT_CLOUD_EVENT_ENCODING_CONFIG = CLOUD_EVENT_STRUCTURED_ENCODING;
  public static final String CLOUD_EVENT_ENCODING_DOC = "Which cloudevent encoding to use. Use structured encoding by default";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(
            EVENT_EXPORTER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            ConfigDef.Importance.HIGH,
            EVENT_EXPORTER_CLASS_DOC
        ).define(
            CLOUD_EVENT_ENCODING_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_CLOUD_EVENT_ENCODING_CONFIG,
            ConfigDef.Importance.LOW,
            CLOUD_EVENT_ENCODING_DOC
        );
  }

  public EventLoggerConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }


}
