package io.confluent.telemetry.exporter.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;

import org.assertj.core.api.Condition;
import org.junit.Test;

public class KafkaExporterConfigTest {

  private final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
      .put(KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
      .put(ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());

  @Test
  public void testTopicConfig() {
    builder
        .put(KafkaExporterConfig.TOPIC_NAME_CONFIG, "topicName")
        .put(KafkaExporterConfig.TOPIC_CREATE_CONFIG, false)
        .put(KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, 2)
        .put(KafkaExporterConfig.TOPIC_PARTITIONS_CONFIG, 4)
        .put(KafkaExporterConfig.TOPIC_RETENTION_MS_CONFIG, 1000);

    KafkaExporterConfig exporterConfig = new KafkaExporterConfig(builder.build());
    assertThat(exporterConfig.getTopicName()).isEqualTo("topicName");
    assertThat(exporterConfig.isCreateTopic()).isFalse();
    assertThat(exporterConfig.getTopicReplicas()).isEqualTo(2);
    assertThat(exporterConfig.getTopicPartitions()).isEqualTo(4);

    Map<String, String> topicConfig = exporterConfig.getTopicConfig();
    assertThat(topicConfig).contains(
        entry(TopicConfig.RETENTION_MS_CONFIG, "1000"),
        entry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
    );
  }

  @Test
  public void testExporterProperties() {
    String prefix = ConfluentTelemetryConfig.exporterPrefixForName("test");
    Map<String, Object> properties = ImmutableMap.<String, Object>builder()
        .put(prefix + "type", "kafka")
        .put(prefix + "producer.bootstrap.servers", "localhost:1234")
        .put(prefix + "producer.security.protocol", "SASL_SSL")
        .put(prefix + "producer.sasl.mechanism", "PLAIN")
        .put(prefix + "producer.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule")
        .put(prefix + "topic.max.message.bytes", 8388608)
        .build();

    ConfluentTelemetryConfig telemetryConfig = new ConfluentTelemetryConfig(properties);
    Map<String, ExporterConfig> exporterConfigs = telemetryConfig.enabledExporters();
    assertThat(exporterConfigs)
      .hasEntrySatisfying("test", new Condition<>(c -> c instanceof KafkaExporterConfig, "is KafkaExporterConfig"));
    KafkaExporterConfig exporterConfig = (KafkaExporterConfig) exporterConfigs.get("test");

    Properties producerProperties = exporterConfig.getProducerProperties();
    assertThat(producerProperties).contains(
        entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234"),
        entry(SaslConfigs.SASL_MECHANISM, "PLAIN"),
        entry(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule")
    );

    assertThat(exporterConfig.getTopicConfig()).contains(
        entry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "8388608")
    );

  }

}