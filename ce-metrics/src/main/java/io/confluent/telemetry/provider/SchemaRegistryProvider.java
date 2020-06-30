// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.telemetry.provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.collector.JvmMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.opencensus.proto.resource.v1.Resource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import static io.confluent.telemetry.provider.Utils.RESOURCE_LABEL_CLUSTER_ID;
import static io.confluent.telemetry.provider.Utils.getResourceLabels;
import static io.confluent.telemetry.provider.Utils.notEmptyString;
import static io.confluent.telemetry.provider.Utils.validateRequiredLabels;

public class SchemaRegistryProvider implements Provider {

  @VisibleForTesting
  public static final String DOMAIN = "io.confluent.schema.registry";
  public static final String NAMESPACE = "kafka.schema.registry";

  public static final String SCHEMAS_TOPIC_CONFIG = "kafkastore.topic";

  private String schemasTopic;
  private Resource resource;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    schemasTopic = (String) configs.get(SCHEMAS_TOPIC_CONFIG);
  }

  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    return notEmptyString(config, SCHEMAS_TOPIC_CONFIG) &&
         validateRequiredLabels(metricsContext.contextLabels());
  }

  /**
   * A callback method that a user can implement to get updates for {@link MetricsContext}.
   *
   * @param metricsContext metrics context
   */
  @Override
  public void contextChange(MetricsContext metricsContext) {
      String clusterId = metricsContext.contextLabels()
              .get(RESOURCE_LABEL_CLUSTER_ID);
      String type = metricsContext.contextLabels().get(ConfluentConfigs.RESOURCE_LABEL_TYPE);
      String version = metricsContext.contextLabels()
              .get(ConfluentConfigs.RESOURCE_LABEL_VERSION);
      ResourceBuilderFacade resourceBuilderFacade =
          new ResourceBuilderFacade(type.toLowerCase(Locale.ROOT))
          .withVersion(version)
          .withId(clusterId)
          // RESOURCE LABELS (prefixed with resource type)
          .withNamespacedLabels(getResourceLabels(metricsContext.contextLabels()));
      if (schemasTopic != null) {
          resourceBuilderFacade.withNamespacedLabel("topic", schemasTopic);
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
