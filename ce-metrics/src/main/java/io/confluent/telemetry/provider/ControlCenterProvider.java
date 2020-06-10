// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.telemetry.provider;

import com.google.common.collect.ImmutableList;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.collector.JvmMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.VolumeMetricsCollector;
import io.opencensus.proto.resource.v1.Resource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static io.confluent.telemetry.provider.Utils.RESOURCE_LABEL_CLUSTER_ID;
import static io.confluent.telemetry.provider.Utils.buildResourceFromLabels;
import static io.confluent.telemetry.provider.Utils.notEmptyString;

public class  ControlCenterProvider implements Provider {
  private static final Logger log = LoggerFactory.getLogger(ControlCenterProvider.class);

  public static final String DOMAIN = "io.confluent.controlcenter";
  public static final String NAMESPACE = "confluent.controlcenter";
  private Resource resource;
  private ConfluentTelemetryConfig config;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    this.config = new ConfluentTelemetryConfig(configs);
  }

  @Override
  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    final Map<String, String> metadata = metricsContext.contextLabels();
    // Ideally we should be using 'validateRequiredLabels' here. In validateRequiredLabels, however,
    // "RESOURCE_LABEL_COMMIT_ID" is a required label, and currently we don't have a way to know
    // commit-id in Control center. Revert this to using validateRequiredLabels once changes have
    // been made in C3 to resolve commitId.
    return notEmptyString(metadata, MetricsContext.NAMESPACE) &&
        notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_TYPE) &&
        notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_VERSION) &&
        notEmptyString(metadata, RESOURCE_LABEL_CLUSTER_ID);
  }

  /**
   * A callback method that a user can implement to get updates for {@link MetricsContext}.
   *
   * @param metricsContext metrics context
   */
  @Override
  public void contextChange(MetricsContext metricsContext) {
    this.resource = buildResourceFromLabels(metricsContext).build();
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
      Context ctx, Predicate<MetricKey> whitelistPredicate) {
    return ImmutableList.of(
        JvmMetricsCollector.newBuilder()
            .setContext(ctx)
            .setMetricWhitelistFilter(whitelistPredicate)
            .build(),
        VolumeMetricsCollector.newBuilder(config)
            .setContext(ctx)
            .setMetricWhitelistFilter(whitelistPredicate)
            .build());
  }
}
