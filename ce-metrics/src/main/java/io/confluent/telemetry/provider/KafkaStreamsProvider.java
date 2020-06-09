// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.telemetry.provider;

import static io.confluent.telemetry.provider.Utils.buildResourceFromLabelsAndClusterId;
import static io.confluent.telemetry.provider.Utils.notEmptyString;
import static io.confluent.telemetry.provider.Utils.validateRequiredLabels;

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

public class KafkaStreamsProvider implements Provider {

  @VisibleForTesting
  public static final String DOMAIN = "io.confluent.kafka.streams";
  public static final String NAMESPACE = "kafka.streams";
  public static final String STREAMS_APPLICATION_ID = "application.id";
  private static final Logger log = LoggerFactory.getLogger(KafkaStreamsProvider.class);
  private Resource resource;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
  }

  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    return notEmptyString(config, STREAMS_APPLICATION_ID) &&
        validateRequiredLabels(metricsContext.contextLabels());
  }

  @Override
  public void contextChange(MetricsContext metricsContext) {
    String streamsApplicationId = metricsContext.contextLabels()
        .get(ConfluentConfigs.RESOURCE_LABEL_PREFIX + STREAMS_APPLICATION_ID);
    ResourceBuilderFacade resourceBuilderFacade = buildResourceFromLabelsAndClusterId(metricsContext, streamsApplicationId);
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
      Context ctx, Predicate<MetricKey> whitelistPredicate) {
    return ImmutableList.of(
        JvmMetricsCollector.newBuilder()
            .setContext(ctx)
            .setMetricWhitelistFilter(whitelistPredicate)
            .build()
    );
  }
}
