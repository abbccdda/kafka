/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit;

import static io.confluent.security.audit.AuditLogConfig.toEventLoggerConfig;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.confluent.events.EventLoggerConfig;
import io.confluent.events.exporter.kafka.KafkaExporter;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

public class AuditLogConfigTest {

  @Test
  public void testToEventLoggerProperties() {
    Map<String, Object> o = toEventLoggerConfig(ImmutableMap.<String, Object>builder()
        .put(AUDIT_PREFIX + EventLoggerConfig.KAFKA_EXPORTER_PREFIX
                + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName())
        .put(AUDIT_PREFIX + EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG,
            KafkaExporter.class.getName())
        .put(AUDIT_EVENT_ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig("foo:9093"))
        .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bar:9093")
        .put(AUDIT_PREFIX + EventLoggerConfig.TOPIC_REPLICAS_CONFIG, "1")
        .build());

    assertTrue(o.containsKey(EventLoggerConfig.TOPIC_REPLICAS_CONFIG));
    assertEquals("1", o.get(EventLoggerConfig.TOPIC_REPLICAS_CONFIG));

    assertTrue(o.containsKey(
        EventLoggerConfig.KAFKA_EXPORTER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer",
        o.get(
            EventLoggerConfig.KAFKA_EXPORTER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));

    assertTrue(o.containsKey(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG));
    assertEquals("io.confluent.events.exporter.kafka.KafkaExporter",
        o.get(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG));

    assertTrue(o.containsKey(EventLoggerConfig.TOPIC_CONFIG));
    assertEquals(o.get(EventLoggerConfig.TOPIC_CONFIG),
        "{\"topics\":[{\"name\":\"confluent-audit-log-events\",\"partitions\":0,\"replicationFactor\":0,\"config\":{\"retention.ms\":\"7776000000\"}}]}");

    assertTrue(o.containsKey(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("foo:9093", o.get(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG));

  }

  @Test
  public void testEventLoggerPropertiesHasClientProps() {
    Map<String, Object> o = toEventLoggerConfig(ImmutableMap.<String, Object>builder()
        .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bar:9093")
        .put(SaslConfigs.SASL_MECHANISM, SaslConfigs.GSSAPI_MECHANISM)
        .put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS)
        .build());

    assertTrue(o.containsKey(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("bar:9093", o.get(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG));

    assertTrue(o.containsKey(EventLoggerConfig.KAFKA_EXPORTER_PREFIX + SaslConfigs.SASL_MECHANISM));
    assertEquals(SaslConfigs.GSSAPI_MECHANISM,
        o.get(EventLoggerConfig.KAFKA_EXPORTER_PREFIX + SaslConfigs.SASL_MECHANISM));

    assertTrue(o.containsKey(
        EventLoggerConfig.KAFKA_EXPORTER_PREFIX + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));
    assertEquals(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS,
        o.get(EventLoggerConfig.KAFKA_EXPORTER_PREFIX + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));


  }

  @Test
  public void testAdminClientProperties() {
    Map<String, Object> o = toEventLoggerConfig(ImmutableMap.<String, Object>builder()
        .put(AUDIT_PREFIX + EventLoggerConfig.KAFKA_EXPORTER_PREFIX
                + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName())
        .put(AUDIT_PREFIX + EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG,
            KafkaExporter.class.getName())
        .put(AUDIT_EVENT_ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig("foo:9093"))
        .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bar:9093")
        .put(AUDIT_PREFIX + EventLoggerConfig.TOPIC_REPLICAS_CONFIG, "1")
        .build());

    EventLoggerConfig config = new EventLoggerConfig(o);

    Properties props = config.clientProperties(AdminClientConfig.configNames());
    assertTrue(props.containsKey("bootstrap.servers"));
    assertFalse(props.containsKey("value.serializer"));
    assertFalse(props.containsKey("topic.config"));

    for (String key : props.stringPropertyNames()) {
      assertTrue(key + " not in expected set", AdminClientConfig.configNames().contains(key));
    }
  }

  @Test
  public void testProducerProperties() {
    Map<String, Object> o = toEventLoggerConfig(ImmutableMap.<String, Object>builder()
        .put(AUDIT_PREFIX + EventLoggerConfig.KAFKA_EXPORTER_PREFIX
                + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName())
        .put(AUDIT_PREFIX + EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG,
            KafkaExporter.class.getName())
        .put(AUDIT_EVENT_ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig("foo:9093"))
        .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bar:9093")
        .put(AUDIT_PREFIX + EventLoggerConfig.TOPIC_REPLICAS_CONFIG, "1")
        .build());

    EventLoggerConfig config = new EventLoggerConfig(o);

    Properties props = config.clientProperties(ProducerConfig.configNames());
    assertTrue(props.containsKey("bootstrap.servers"));
    assertTrue(props.containsKey("value.serializer"));
    assertFalse(props.containsKey("topic.config"));

    for (String key : props.stringPropertyNames()) {
      assertTrue(key + " not in expected set", ProducerConfig.configNames().contains(key));
    }
  }


}
