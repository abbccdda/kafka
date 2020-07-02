package io.confluent.telemetry.provider;

import io.opencensus.proto.resource.v1.Resource;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaServerProviderTest {
    @Test
    public void testValidate() {
        Provider serverProvider = new KafkaServerProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "kafka");
        contextLabels.put(Utils.KAFKA_CLUSTER_ID, "cluster-1");
        contextLabels.put(Utils.KAFKA_BROKER_ID, "cluster-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.server", contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(KafkaConfig.BrokerIdProp(), "broker-1");
        Assert.assertTrue(serverProvider.validate(metricsContext, config));
    }

    @Test
    public void testContextChange() {
        Provider serverProvider = new KafkaServerProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "kafka");
        contextLabels.put(Utils.KAFKA_CLUSTER_ID, "cluster-1");
        contextLabels.put(Utils.KAFKA_BROKER_ID, "broker-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.server", contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(KafkaConfig.BrokerIdProp(), "broker-1");

        serverProvider.configure(config);
        serverProvider.contextChange(metricsContext);
        Resource resource = serverProvider.resource();
        Assert.assertEquals("kafka", resource.getType());
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("kafka.id"));
        Assert.assertEquals("broker-1", resource.getLabelsOrThrow("kafka.broker.id"));
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("kafka.cluster.id"));
        Assert.assertEquals("kafka", resource.getLabelsOrThrow("kafka.type"));
    }
}
