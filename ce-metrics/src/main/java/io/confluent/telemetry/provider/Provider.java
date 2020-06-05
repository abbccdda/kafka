// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.telemetry.provider;

import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.collector.MetricsCollector;
import io.opencensus.proto.resource.v1.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.MetricsContext;

/**
 * Implement this interface to collect metrics for your component. You will need to register your
 * implementation in {@link io.confluent.telemetry.provider.ProviderRegistry}.
 */
public interface Provider extends Configurable {

  /**
   * Validate that all the data required for generating correct metrics is present. The provider
   * will be disabled if validation fails.
   *
   * @param metricsContext {@link MetricsContext}
   * @return false if all the data required for generating correct metrics is missing, true
   * otherwise.
   */
  boolean validate(MetricsContext metricsContext, Map<String, ?> config);

  /**
   * Domain of the active provider. This is used by other parts of the reporter.
   *
   * @return Domain in string format.
   */
  String domain();

  /**
   * The resource for this provider.
   *
   * @return A fully formed {@link Resource} will all the tags.
   */
  Resource resource();

  /**
   * Sets the metrics labels for the service or library exposing metrics. This will be called before {@link org.apache.kafka.common.metrics.MetricsReporter#init(List)} and may be called anytime after that.
   *
   * @param metricsContext {@link MetricsContext}
   */
  void contextChange(MetricsContext metricsContext);

  /**
   * The collector for Kafka Metrics library is enabled by default. If you need any more, add them
   * here.
   *
   * @param ctx {@link Context}
   * @return List of extra collectors
   */
  default List<MetricsCollector> extraCollectors(Context ctx,
      Predicate<MetricKey> whitelistPredicate) {
    return Collections.emptyList();
  }
}
