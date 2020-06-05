// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.telemetry.provider;

import static io.confluent.telemetry.provider.Utils.KAFKA_BROKER_ID;
import static io.confluent.telemetry.provider.Utils.KAFKA_CLUSTER_ID;
import static io.confluent.telemetry.provider.Utils.getResourceLabels;
import static io.confluent.telemetry.provider.Utils.notEmptyString;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.collector.CPUMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.VolumeMetricsCollector;
import io.confluent.telemetry.collector.YammerMetricsCollector;
import io.opencensus.proto.resource.v1.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import kafka.metrics.KafkaYammerMetrics;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaServerProvider implements Provider {

  @VisibleForTesting
  public static final String LABEL_CLUSTER_ID = "cluster.id";
  @VisibleForTesting
  public static final String LABEL_BROKER_ID = "broker.id";
  @VisibleForTesting
  public static final String LABEL_BROKER_RACK = "broker.rack";
  public static final String NAMESPACE = "kafka.server";
  private static final Logger log = LoggerFactory.getLogger(KafkaServerProvider.class);
  private static final String DOMAIN = "io.confluent.kafka.server";
  private Resource resource;
  private ConfluentTelemetryConfig config;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    this.config = new ConfluentTelemetryConfig(configs);
  }

  @Override
  public Resource resource() {
    return this.resource;
  }

  @Override
  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    // Add all labels that should be present to generate the correct metrics.
    return notEmptyString(config, KafkaConfig.BrokerIdProp()) &&
            validateKafkaServerRequiredLabels(metricsContext.contextLabels());
  }

  @Override
  public String domain() {
    return DOMAIN;
  }

  public List<MetricsCollector> extraCollectors(Context ctx,
      Predicate<MetricKey> whitelistPredicate) {

    List<MetricsCollector> collectors = new ArrayList<>();

    collectors.add(
        CPUMetricsCollector.newBuilder()
            .setContext(ctx)
            .setMetricWhitelistFilter(whitelistPredicate)
            .build()
    );

    collectors.add(
        VolumeMetricsCollector.newBuilder(config)
            .setContext(ctx)
            .setMetricWhitelistFilter(whitelistPredicate)
            .build()
    );

    collectors.add(
        YammerMetricsCollector.newBuilder()
            .setContext(ctx)
            .setMetricsRegistry(KafkaYammerMetrics.defaultRegistry())
            .setMetricWhitelistFilter(whitelistPredicate)
            .build()
    );

    return collectors;
  }

  @Override
  public void contextChange(MetricsContext metricsContext) {
    String clusterId = metricsContext.contextLabels()
        .get(Utils.KAFKA_CLUSTER_ID);
    String brokerId = metricsContext.contextLabels().get(Utils.KAFKA_BROKER_ID);
    String type = metricsContext.contextLabels().get(ConfluentConfigs.RESOURCE_LABEL_TYPE);
    String version = metricsContext.contextLabels()
        .get(ConfluentConfigs.RESOURCE_LABEL_VERSION);
    ResourceBuilderFacade resourceBuilderFacade = new ResourceBuilderFacade(type.toLowerCase(Locale.ROOT))
        .withVersion(version)
        .withId(clusterId)
        // RESOURCE LABELS (prefixed with resource type)
        .withNamespacedLabels(getResourceLabels(metricsContext.contextLabels()))
        // TODO: Add these to the context.
        .withNamespacedLabel(LABEL_BROKER_ID, brokerId)
        .withNamespacedLabel(LABEL_CLUSTER_ID, clusterId)
        .withLabels(config.getLabels());

    // Do not add kafka.broker.rack if data is unavailable.
    config.getBrokerRack()
        .ifPresent(value -> resourceBuilderFacade.withNamespacedLabel(LABEL_BROKER_RACK, value));
    // ---- Legacy cloud labels

    this.resource = resourceBuilderFacade.build();
  }

  private boolean validateKafkaServerRequiredLabels(Map<String, String> metadata) {
    return Utils.validateRequiredResourceLabels(metadata) &&
            notEmptyString(metadata, KAFKA_CLUSTER_ID) &&
            notEmptyString(metadata, KAFKA_BROKER_ID);
  }

}
