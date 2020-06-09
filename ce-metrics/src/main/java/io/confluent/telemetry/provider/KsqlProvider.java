package io.confluent.telemetry.provider;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.telemetry.ResourceBuilderFacade;;
import com.google.common.collect.ImmutableList;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.collector.JvmMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.opencensus.proto.resource.v1.Resource;
import java.util.List;
import java.util.function.Predicate;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.confluent.telemetry.provider.Utils.buildResourceFromLabelsAndClusterId;
import static io.confluent.telemetry.provider.Utils.notEmptyString;
import static io.confluent.telemetry.provider.Utils.validateRequiredLabels;

public class KsqlProvider implements Provider {

  @VisibleForTesting
  public static final String DOMAIN = "io.confluent.ksql";
  public static final String NAMESPACE = "io.confluent.ksql.metrics";
  public static final String KSQL_SERVICE_ID = "ksql.service.id";
  private static final Logger log = LoggerFactory.getLogger(KsqlProvider.class);
  private Resource resource;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
  }

  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    return notEmptyString(config, KSQL_SERVICE_ID) &&
        validateRequiredLabels(metricsContext.contextLabels());
  }

  @Override
  public void contextChange(MetricsContext metricsContext) {
    String ksqlClusterId = metricsContext.contextLabels()
        .get(ConfluentConfigs.RESOURCE_LABEL_PREFIX + KSQL_SERVICE_ID);
    ResourceBuilderFacade resourceBuilderFacade = buildResourceFromLabelsAndClusterId(metricsContext, ksqlClusterId);
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
