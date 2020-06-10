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

public class KsqlProviderTest {
  @Test
  public void testValidate() {
    Provider ksqlProvider = new KsqlProvider();
    Map<String, Object> contextLabels = new HashMap<>();
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "ksql");
    contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "cluster-1");
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
    MetricsContext metricsContext = new KafkaMetricsContext(KsqlProvider.DOMAIN, contextLabels);
    Map<String, Object> config = new HashMap<>();
    config.put(KsqlProvider.KSQL_SERVICE_ID, "app-1");
    Assert.assertTrue(ksqlProvider.validate(metricsContext, config));
  }

  @Test
  public void testContextChange() {
    Provider ksqlProvider = new KsqlProvider();
    String serviceId =  "cluster-1";
    Map<String, Object> contextLabels = new HashMap<>();
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "ksql");
    contextLabels.put(Utils.RESOURCE_LABEL_CLUSTER_ID, serviceId);
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
    MetricsContext metricsContext = new KafkaMetricsContext(KsqlProvider.DOMAIN, contextLabels);
    Map<String, Object> config = new HashMap<>();
    config.put(KsqlProvider.KSQL_SERVICE_ID, serviceId);
    ksqlProvider.configure(config);
    ksqlProvider.contextChange(metricsContext);
    Resource resource = ksqlProvider.resource();
    Assert.assertEquals("ksql", resource.getType());
    Assert.assertEquals(serviceId, resource.getLabelsOrThrow("ksql.cluster.id"));
    Assert.assertEquals(serviceId, resource.getLabelsOrThrow("ksql.id"));
    Assert.assertEquals("ksql", resource.getLabelsOrThrow("ksql.type"));
  }
}

