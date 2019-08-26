package io.confluent.telemetry.exporter.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Test;

public class KafkaExporterConfigTest {

  private final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
      .put(KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");

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
  public void testDeprecatedProperties() {
    // See https://github.com/confluentinc/cc-cluster-spec-service/blob/master/plugins/kafka/templates/serverConfig.tmpl#L75-L92
    // for properties for which we must ensure backwards compatibility
    Map<String, Object> properties = ImmutableMap.<String, Object>builder()
        .put("confluent.telemetry.metrics.reporter.bootstrap.servers", "localhost:1234")
        .put("confluent.telemetry.metrics.reporter.security.protocol", "SASL_SSL")
        .put("confluent.telemetry.metrics.reporter.sasl.mechanism", "PLAIN")
        .put("confluent.telemetry.metrics.reporter.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule")
        .put("confluent.telemetry.metrics.reporter.topic.max.message.bytes", 8388608)
        .build();

    ConfluentTelemetryConfig telemetryConfig = new ConfluentTelemetryConfig(properties);
    KafkaExporterConfig exporterConfig = telemetryConfig.getKafkaExporterConfig();

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