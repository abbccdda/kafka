package io.confluent.security.audit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.security.audit.appender.LogEventAppender;
import java.io.StringWriter;
import java.util.HashMap;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Test;

public class LogEventAppenderTest {

  private KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private AuditLogEntry entry = AuditLogEntry.newBuilder()
      .setAuthenticationInfo(AuthenticationInfo.newBuilder()
          .setPrincipal(alice.toString())
          .build())
      .build();
  private CloudEvent event = CloudEventUtils
      .wrap("event_type", "crn://authority/kafka=source", "crn://authority/kafka=subject", entry);

  @Test
  public void testNamedLoggers() {
    HashMap<String, String> config1 = new HashMap<>();
    config1.put(EventLogConfig.EVENT_LOG_NAME_CONFIG, "test1");
    config1.put(EventLogConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    HashMap<String, String> config2 = new HashMap<>();
    config2.put(EventLogConfig.EVENT_LOG_NAME_CONFIG, "test2");
    config2.put(EventLogConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    EventLogger logger1 = EventLogger.logger("config1", config1);
    EventLogger logger2 = EventLogger.logger("config2", config2);
    EventLogger logger1a = EventLogger.logger("config1", config1);

    Assert.assertEquals(logger1, logger1a);
    Assert.assertNotEquals(logger1, logger2);
  }

  @Test
  public void testLogToLog4J() throws Exception {
    Logger testLogger = Logger.getLogger("test");
    StringWriter writer = new StringWriter();
    testLogger.removeAllAppenders();
    testLogger.addAppender(new WriterAppender(new PatternLayout("%m"), writer));

    HashMap<String, String> config = new HashMap<>();
    config.put(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG, LogEventAppender.class.getCanonicalName());
    config.put(EventLogConfig.EVENT_LOG_NAME_CONFIG, "test");
    config.put(EventLogConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    EventLogger logger = EventLogger.logger("testLogToLog4J", config);

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
    config.put(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG, LogEventAppender.class.getCanonicalName());
    config.put(EventLogConfig.EVENT_LOG_NAME_CONFIG, "test");
    config.put(EventLogConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    EventLogger logger = EventLogger.logger("testReconfigure", config);

    logger.log(event);

    String logText = writer.toString();
    // make sure that something from the envelope is there
    assertTrue(logText.contains("\"type\":\"event_type\""));
    // make sure that something from the data is there
    assertTrue(logText.contains("User:Alice"));
    assertEquals("", writer2.toString());

    config.put(EventLogConfig.EVENT_LOG_NAME_CONFIG, "test2");
    logger.reconfigure(config);

    logger.log(event);

    String logText2 = writer2.toString();
    // should be identical
    assertEquals(logText, logText2);

    // make sure the original writer didn't get the second write...
    assertEquals(logText, writer.toString());
  }
}
