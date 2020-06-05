package io.confluent.telemetry.provider;

import io.opencensus.proto.resource.v1.Resource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaConnectProviderTest {
    @Test
    public void testValidate() {
        Provider connectProvider = new KafkaConnectProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "connect");
        contextLabels.put(Utils.CONNECT_KAFKA_CLUSTER_ID, "cluster-1");
        contextLabels.put(Utils.CONNECT_GROUP_ID, "group-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.connect", contextLabels);
        Map<String, Object> config = new HashMap<>();
        Assert.assertTrue(connectProvider.validate(metricsContext, config));
    }

    @Test
    public void testContextChange() {
        Provider connectProvider = new KafkaConnectProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "connect");
        contextLabels.put(Utils.CONNECT_KAFKA_CLUSTER_ID, "cluster-1");
        contextLabels.put(Utils.CONNECT_GROUP_ID, "group-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.connect", contextLabels);
        Map<String, Object> config = new HashMap<>();
        connectProvider.configure(config);
        connectProvider.contextChange(metricsContext);
        Resource resource = connectProvider.resource();
        Assert.assertEquals("connect", resource.getType());
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("connect.id"));
        Assert.assertEquals("group-1", resource.getLabelsOrThrow("connect.group.id"));
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("connect.kafka.cluster.id"));
        Assert.assertEquals("connect", resource.getLabelsOrThrow("connect.type"));
    }
}
