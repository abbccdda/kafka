/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.cloudevents.CloudEvent;
import io.confluent.events.exporter.LogExporter;
import io.confluent.events.test.SomeMessage;
import java.io.StringWriter;
import java.util.HashMap;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Test;

public class LogExporterTest {

  private KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private SomeMessage entry = SomeMessage.newBuilder()
      .setPrincipal(alice.toString())
      .build();

  private CloudEvent event = ProtobufEvent.newBuilder()
      .setType("event_type")
      .setSource("crn://authority/kafka=source")
      .setSubject("crn://authority/kafka=subject")
      .setEncoding(EventLoggerConfig.DEFAULT_CLOUD_EVENT_ENCODING_CONFIG)
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
    config.put(EventLoggerConfig.LOG_EVENT_EXPORTER_NAME_CONFIG, "test");
    config.put(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    EventLogger logger = new EventLogger();

    logger.configure(config);
    logger.log(event);

    String logText = writer.toString();
    // make sure that something from the envelope is there
    assertTrue(logText.contains("\"type\":\"event_type\""));
    // make sure that something from the data is there
    assertTrue(logText.contains("User:Alice"));
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
    config.put(EventLoggerConfig.LOG_EVENT_EXPORTER_NAME_CONFIG, "test");
    config.put(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    EventLogger logger = new EventLogger();
    logger.configure(config);
    logger.log(event);

    String logText = writer.toString();
    // make sure that something from the envelope is there
    assertTrue(logText.contains("\"type\":\"event_type\""));
    // make sure that something from the data is there
    assertTrue(logText.contains("User:Alice"));
    assertEquals("", writer2.toString());

    config.put(EventLoggerConfig.LOG_EVENT_EXPORTER_NAME_CONFIG, "test2");
    logger.reconfigure(config);

    logger.log(event);

    String logText2 = writer2.toString();
    // should be identical
    assertEquals(logText, logText2);

    // make sure the original writer didn't get the second write...
    assertEquals(logText, writer.toString());
  }
}
