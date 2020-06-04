/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events.exporter.log;

import com.google.protobuf.MessageLite;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.telemetry.events.serde.Serializer;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.exporter.Exporter;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LogExporter sends events to the SLF4J-configured log file
 */

public class LogExporter<T extends MessageLite> implements Exporter<T> {

  private Logger log = LoggerFactory.getLogger(LogExporter.class);
  private Serializer<T> ser;

  @Override
  public void configure(Map<String, ?> configs) {
    LogExporterConfig config = new LogExporterConfig(configs);
    log = LoggerFactory
        .getLogger(config.getString(LogExporterConfig.LOG_EVENT_EXPORTER_NAME_CONFIG));

    ser = Protobuf.structuredSerializer();
  }

  @Override
  public void append(CloudEvent<AttributesImpl, T> event) throws RuntimeException {
    log.info(new String(ser.serialize(event)));
  }

  @Override
  public boolean routeReady(CloudEvent<AttributesImpl, T> event) {
    return true;
  }


  @Override
  public void close() throws Exception {

  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Utils.mkSet(LogExporterConfig.LOG_EVENT_EXPORTER_NAME_CONFIG);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    configure(configs);
  }
}
