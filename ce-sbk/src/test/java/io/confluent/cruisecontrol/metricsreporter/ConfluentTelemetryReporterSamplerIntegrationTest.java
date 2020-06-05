package io.confluent.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import io.confluent.metrics.reporter.integration.MetricReporterClusterTestHarness;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager.BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class ConfluentTelemetryReporterSamplerIntegrationTest extends MetricReporterClusterTestHarness {
    private ConfluentTelemetryReporterSampler sampler = new ConfluentTelemetryReporterSampler();

    @Test
    public void testSampler() throws MetricSamplingException, InterruptedException {
        Map<String, Object> resolverConfig = new HashMap<>();
        String capacityConfigFile =
                KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
        resolverConfig.put(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
        BrokerCapacityConfigResolver resolver = new BrokerCapacityConfigFileResolver();
        resolver.configure(resolverConfig);

        MetadataCache metadataCache = servers.get(0).metadataCache();

        TestUtils.waitForCondition(() -> metadataCache.getClusterMetadata(zkClient.getClusterId().get(),
                kafkaConfig(0).interBrokerListenerName()).topics().contains(KafkaExporterConfig.DEFAULT_TOPIC_NAME),
                600000, "Metrics topic " + KafkaExporterConfig.DEFAULT_TOPIC_NAME + " does not exist");

        Properties props = new Properties();
        props.put(ConfluentMetricsSamplerBase.METRIC_REPORTER_TOPIC_PATTERN, KafkaExporterConfig.DEFAULT_TOPIC_NAME);
        props.put(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);
        props.put(BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG, resolver);
        sampler.configure(new KafkaCruiseControlConfig(props).mergedConfigValues());

        Cluster cluster = metadataCache.getClusterMetadata(zkClient.getClusterId().get(),
                kafkaConfig(0).interBrokerListenerName());
        Set<TopicPartition> partitions = new HashSet<>();
        for (String topic : cluster.topics()) {
            List<PartitionInfo> partitionsForTopic = cluster.partitionsForTopic(topic);
            for (PartitionInfo partitionInfo : partitionsForTopic) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        MetricSampler.Samples samples = sampler.getSamples(cluster, partitions, 0L, System.currentTimeMillis(), MetricSampler.SamplingMode.ALL,
                KafkaMetricDef.commonMetricDef(), 300000L);

        // MetricReporterClusterTestHarness reports metrics from only broker 1
        // Check that there is at least one partition sample and a broker sample for broker 1
        // The presence of a broker sample tests that all required metrics have been sampled, because Cruise Control will
        // skip it otherwise
        assertFalse(samples.partitionMetricSamples().isEmpty());
        assertTrue(samples.brokerMetricSamples().size() >= 1 &&
                samples.brokerMetricSamples().stream().anyMatch(sample -> sample.entity().brokerId() == 1));
    }

    @Override
    protected void injectMetricReporterProperties(Properties props, String brokerList) {
        props.setProperty(KafkaConfig.MetricReporterClassesProp(), "io.confluent.telemetry.reporter.KafkaServerMetricsReporter");

        // disable the default exporters
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
                + ExporterConfig.ENABLED_CONFIG, "false");

        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME) + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME) + ExporterConfig.ENABLED_CONFIG, "true");
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME) + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME) + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");
        props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
        props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
        props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "region", "test");
        props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "pkc", "pkc-bar");
        props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

        // force flush every message so that we can generate some Yammer timer metrics
        props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
    }
}
