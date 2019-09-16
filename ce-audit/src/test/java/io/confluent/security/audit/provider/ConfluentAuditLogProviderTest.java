package io.confluent.security.audit.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.EventLogger;
import io.confluent.security.audit.appender.KafkaEventAppender;
import io.confluent.security.audit.appender.LogEventAppender;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

public class ConfluentAuditLogProviderTest {

  @Test
  public void testAuditLogProviderConfig() {
    Map<String, Object> configs = Collections.singletonMap(
        EventLogConfig.EVENT_LOGGER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:9092");
    AuditLogProvider provider = ConfluentBuiltInProviders.loadAuditLogProvider(configs);
    provider.configure(configs);
    assertEquals(ConfluentAuditLogProvider.class, provider.getClass());
    ConfluentAuditLogProvider confluentProvider = (ConfluentAuditLogProvider) provider;
    EventLogger fileLogger = confluentProvider.localFileLogger();
    assertNotNull(fileLogger);
    assertEquals(LogEventAppender.class, TestUtils.fieldValue(fileLogger, EventLogger.class, "eventAppender").getClass());
    EventLogger kafkaLogger = confluentProvider.kafkaLogger();
    assertNotNull(kafkaLogger);
    assertEquals(KafkaEventAppender.class, TestUtils.fieldValue(kafkaLogger, EventLogger.class, "eventAppender").getClass());

    AuditLogProvider defaultProvider = ConfluentBuiltInProviders.loadAuditLogProvider(Collections.emptyMap());
    assertEquals(DefaultAuditLogProvider.class, defaultProvider.getClass());
  }
}