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

public class SchemaRegistryProviderTest {

    @Test
    public void testValidate() {
        Provider serverProvider = new SchemaRegistryProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "schemaregistry");
        contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "cluster-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("schemaregistry", contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryProvider.SCHEMAS_TOPIC_CONFIG, "_schemasTopic");
        Assert.assertTrue(serverProvider.validate(metricsContext, config));
    }

    @Test
    public void testContextChange() {
        Provider serverProvider = new SchemaRegistryProvider();
        Map<String, Object> contextLabels = new HashMap<>();
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "schemaregistry");
        contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "cluster-1");
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
        contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
        MetricsContext metricsContext = new KafkaMetricsContext("schemaregistry", contextLabels);
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryProvider.SCHEMAS_TOPIC_CONFIG, "_schemasTopic");
        serverProvider.configure(config);
        serverProvider.contextChange(metricsContext);
        Resource resource = serverProvider.resource();
        Assert.assertEquals("schemaregistry", resource.getType());
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("schemaregistry.id"));
        Assert.assertEquals("cluster-1", resource.getLabelsOrThrow("schemaregistry.cluster.id"));
        Assert.assertEquals("schemaregistry", resource.getLabelsOrThrow("schemaregistry.type"));
        Assert.assertEquals("_schemasTopic", resource.getLabelsOrThrow("schemaregistry.topic"));
    }
}
