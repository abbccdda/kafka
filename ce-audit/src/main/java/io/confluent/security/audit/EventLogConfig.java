package io.confluent.security.audit;

import io.confluent.security.audit.appender.LogEventAppender;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class EventLogConfig extends AbstractConfig {

  public static final String EVENT_LOGGER_PREFIX = "confluent.security.event.logger.";

  public static final String EVENT_LOGGER_CLASS_CONFIG = EVENT_LOGGER_PREFIX + "class";
  public static final String DEFAULT_EVENT_LOGGER_CLASS_CONFIG =
      LogEventAppender.class.getCanonicalName();
  public static final String EVENT_LOGGER_CLASS_DOC = "Class to use for delivering event logs.";

  // Logger name
  public static final String EVENT_LOG_NAME_CONFIG = EVENT_LOGGER_PREFIX + "name";
  public static final String DEFAULT_EVENT_LOG_NAME_CONFIG = "event";
  public static final String EVENT_LOG_NAME_DOC = "Name for the logger, used in getLogger()";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(
            EVENT_LOGGER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            DEFAULT_EVENT_LOGGER_CLASS_CONFIG,
            ConfigDef.Importance.HIGH,
            EVENT_LOGGER_CLASS_DOC
        ).define(
            EVENT_LOG_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_EVENT_LOG_NAME_CONFIG,
            ConfigDef.Importance.LOW,
            EVENT_LOG_NAME_DOC
        );
  }

  public EventLogConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }


}
