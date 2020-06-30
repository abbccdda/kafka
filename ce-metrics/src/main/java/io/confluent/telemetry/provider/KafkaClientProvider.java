// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.telemetry.provider;

import static io.confluent.telemetry.provider.Utils.buildResourceFromLabelsAndClusterId;
import static io.confluent.telemetry.provider.Utils.notEmptyString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.collector.JvmMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.opencensus.proto.resource.v1.Resource;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClientProvider implements Provider {
  @VisibleForTesting
  public static final String DOMAIN = "io.confluent.kafka.client";
  public static final String LABEL_CLIENT_ID = "client.id";
  public static final String ADMIN_NAMESPACE = "kafka.admin.client";
  public static final String CONSUMER_NAMESPACE = "kafka.consumer";
  public static final String PRODUCER_NAMESPACE = "kafka.producer";
  private static final Logger log = LoggerFactory.getLogger(KafkaClientProvider.class);
  private Resource resource;
  private ConfluentTelemetryConfig config;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    this.config = new ConfluentTelemetryConfig(configs);
  }

  @Override
  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    return notEmptyString(config, CommonClientConfigs.CLIENT_ID_CONFIG) &&
        validateRequiredLabels(metricsContext.contextLabels());
  }

  @Override
  public void contextChange(MetricsContext metricsContext) {
    String clusterId = metricsContext.contextLabels()
            .get(Utils.RESOURCE_LABEL_CLUSTER_ID);
    if (clusterId == null) {
      clusterId = metricsContext.contextLabels().get(Utils.CONNECT_KAFKA_CLUSTER_ID);
    }
    this.resource = buildResourceFromLabelsAndClusterId(metricsContext, clusterId)
        .withNamespacedLabel(LABEL_CLIENT_ID,
            (String) this.config.originals().get(CommonClientConfigs.CLIENT_ID_CONFIG))
        .build();
  }

  @Override
  public Resource resource() {
    return this.resource;
  }

  @Override
  public String domain() {
    return DOMAIN;
  }

  private  boolean validateRequiredLabels(Map<String, String> metadata) {
    return Utils.validateRequiredResourceLabels(metadata) &&
            (notEmptyString(metadata, Utils.RESOURCE_LABEL_CLUSTER_ID) || notEmptyString(metadata, Utils.CONNECT_KAFKA_CLUSTER_ID));
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
