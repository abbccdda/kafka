package io.confluent.security.audit.appender;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LogEventAppender sends events to the SLF4J-configured log file
 */
public class LogEventAppender implements EventAppender {

  private Logger log = LoggerFactory.getLogger(LogEventAppender.class);

  @Override
  public void configure(Map<String, ?> configs) {
    EventLogConfig config = new EventLogConfig(configs);
    log = LoggerFactory.getLogger(config.getString(EventLogConfig.EVENT_LOG_NAME_CONFIG));
  }

  @Override
  public void append(CloudEvent event) throws RuntimeException {
    try {
      log.info(CloudEventUtils.toJsonString(event));
    } catch (InvalidProtocolBufferException e) {
      log.warn("Failed to log Audit Log Message", e);
    }
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Utils.mkSet(EventLogConfig.EVENT_LOG_NAME_CONFIG);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    configure(configs);
  }
}
