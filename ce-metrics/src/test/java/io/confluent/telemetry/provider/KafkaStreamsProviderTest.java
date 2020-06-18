package io.confluent.telemetry.provider;

import io.opencensus.proto.resource.v1.Resource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaStreamsProviderTest {
    @Test
    public void testValidate() {
        Provider streamsProvider = new KafkaStreamsProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "streams");
        contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "cluster-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext(KafkaStreamsProvider.NAMESPACE, contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-1");
        Assert.assertTrue(streamsProvider.validate(metricsContext, config));
    }

    @Test
    public void testContextChange() {
        Provider streamsProvider = new KafkaStreamsProvider();
        String applicationId =  "app-1";
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "streams");
        contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "cluster-id");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext(KafkaStreamsProvider.NAMESPACE, contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProvider.configure(config);
        streamsProvider.contextChange(metricsContext);
        Resource resource = streamsProvider.resource();
        Assert.assertEquals("streams", resource.getType());
        Assert.assertEquals("cluster-id", resource.getLabelsOrThrow("streams.id"));
        Assert.assertEquals("cluster-id", resource.getLabelsOrThrow("streams.cluster.id"));
        Assert.assertEquals(applicationId, resource.getLabelsOrThrow("streams.application.id"));
        Assert.assertEquals("streams", resource.getLabelsOrThrow("streams.type"));
    }
}

