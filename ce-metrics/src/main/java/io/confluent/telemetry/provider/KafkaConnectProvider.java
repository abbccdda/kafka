// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.telemetry.provider;

import static io.confluent.telemetry.provider.Utils.CONNECT_KAFKA_CLUSTER_ID;
import static io.confluent.telemetry.provider.Utils.buildResourceFromLabelsAndClusterId;
import static io.confluent.telemetry.provider.Utils.notEmptyString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.collector.JvmMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.opencensus.proto.resource.v1.Resource;

import java.util.List;
import java.util.Map;

import java.util.function.Predicate;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConnectProvider implements Provider {

  @VisibleForTesting
  public static final String DOMAIN = "io.confluent.kafka.connect";
  public static final String NAMESPACE = "kafka.connect";
  public static final String LABEL_GROUP_ID = "group.id";
  public static final String LABEL_KAFKA_CLUSTER_ID = "kafka.cluster.id";
  private static final Logger log = LoggerFactory.getLogger(KafkaConnectProvider.class);
  private Resource resource;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
  }

  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    return validateKafkaConnectRequiredLabels(metricsContext.contextLabels());
  }

  @Override
  public void contextChange(MetricsContext metricsContext) {
    String clusterId = metricsContext.contextLabels()
            .get(Utils.CONNECT_KAFKA_CLUSTER_ID);
    String groupId = metricsContext.contextLabels()
            .get(Utils.CONNECT_GROUP_ID);
    ResourceBuilderFacade resourceBuilderFacade = buildResourceFromLabelsAndClusterId(metricsContext, clusterId);
    resourceBuilderFacade.withNamespacedLabel(LABEL_KAFKA_CLUSTER_ID, clusterId);

    if (groupId != null) {
      resourceBuilderFacade.withNamespacedLabel(LABEL_GROUP_ID, groupId);
    }
    this.resource = resourceBuilderFacade.build();
  }

  @Override
  public Resource resource() {
    return this.resource;
  }

  @Override
  public String domain() {
    return DOMAIN;
  }

  private boolean validateKafkaConnectRequiredLabels(Map<String, String> metadata) {
    return notEmptyString(metadata, MetricsContext.NAMESPACE) &&
            notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_TYPE) &&
            notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_VERSION) &&
            notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID) &&
            notEmptyString(metadata, CONNECT_KAFKA_CLUSTER_ID);
  }

  @Override
  public List<MetricsCollector> extraCollectors(
      Context ctx, Predicate<MetricKey> metricsPredicate) {
    return ImmutableList.of(
        JvmMetricsCollector.newBuilder()
            .setContext(ctx)
            .setMetricsPredicate(metricsPredicate)
            .build()
    );
  }
}
