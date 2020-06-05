package io.confluent.telemetry.provider;

import io.opencensus.proto.resource.v1.Resource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaClientProviderTest {
    @Test
    public void testValidate() {
        Provider clientProvider = new KafkaClientProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "client");
        contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "cluster-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.consumer", contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "client-1");
        Assert.assertTrue(clientProvider.validate(metricsContext, config));
    }

    @Test
    public void testContextChange() {
        Provider clientProvider = new KafkaClientProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "client");
        contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "cluster-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.consumer", contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "client-1");
        clientProvider.configure(config);
        clientProvider.contextChange(metricsContext);
        Resource resource = clientProvider.resource();
        Assert.assertEquals("client", resource.getType());
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("client.id"));
        Assert.assertEquals("client-1", resource.getLabelsOrThrow("client.client.id"));
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("client.cluster.id"));
        Assert.assertEquals("client", resource.getLabelsOrThrow("client.type"));
    }
}
