/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events.exporter.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.telemetry.events.Event;
import io.confluent.telemetry.events.EventLogger;
import io.confluent.telemetry.events.EventLoggerConfig;
import io.confluent.telemetry.events.Test.AMessage;
import io.confluent.telemetry.events.Test.AnEnum;
import io.confluent.telemetry.events.serde.Protobuf;
import java.io.StringWriter;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Test;

public class LogExporterTest {

  private final AMessage entry = AMessage.newBuilder()
      .setALong(1029309)
      .setAString("my string")
      .setAnEnum(AnEnum.A)
      .setSecondString("second string")
      .build();

  private CloudEvent<AttributesImpl, AMessage> event = Event.<AMessage>newBuilder()
      .setType("event_type")
      .setSource("crn://authority/serde=source")
      .setSubject("crn://authority/serde=subject")
      .setDataContentType(Protobuf.APPLICATION_PROTOBUF)
      .setData(entry)
      .build();


  @Test
  public void testLogToLog4J() throws Exception {
    Logger testLogger = Logger.getLogger("test");
    StringWriter writer = new StringWriter();
    testLogger.removeAllAppenders();
    testLogger.addAppender(new WriterAppender(new PatternLayout("%m"), writer));

    assertTrue(testLogger.isInfoEnabled());
    HashMap<String, String> config = new HashMap<>();
    config.put(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, LogExporter.class.getCanonicalName());
    config.put(LogExporterConfig.LOG_EVENT_EXPORTER_NAME_CONFIG, "test");

    EventLogger<AMessage> logger = new EventLogger<>();

    logger.configure(config);
    logger.log(event);

    String logText = writer.toString();
    // make sure that something from the envelope is there
    assertTrue(logText.contains("\"type\":\"event_type\""));
    // make sure that something from the data is there
    assertTrue(logText.contains("my string"));
  }

  @Test
  public void testReconfigure() throws Exception {
    Logger testLogger = Logger.getLogger("test");
    StringWriter writer = new StringWriter();
    testLogger.removeAllAppenders();
    testLogger.addAppender(new WriterAppender(new PatternLayout("%m"), writer));

    Logger testLogger2 = Logger.getLogger("test2");
    StringWriter writer2 = new StringWriter();
    testLogger2.removeAllAppenders();
    testLogger2.addAppender(new WriterAppender(new PatternLayout("%m"), writer2));

    HashMap<String, String> config = new HashMap<>();
    config.put(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, LogExporter.class.getCanonicalName());
    config.put(LogExporterConfig.LOG_EVENT_EXPORTER_NAME_CONFIG, "test");

    EventLogger<AMessage> logger = new EventLogger<>();
    logger.configure(config);
    logger.log(event);

    String logText = writer.toString();
    // make sure that something from the envelope is there
    assertTrue(logText.contains("\"type\":\"event_type\""));
    // make sure that something from the data is there
    assertTrue(logText.contains("my string"));
    assertEquals("", writer2.toString());

    config.put(LogExporterConfig.LOG_EVENT_EXPORTER_NAME_CONFIG, "test2");
    logger.reconfigure(config);

    logger.log(event);

    String logText2 = writer2.toString();
    // should be identical
    assertEquals(logText, logText2);

    // make sure the original writer didn't get the second write...
    assertEquals(logText, writer.toString());
  }
}
