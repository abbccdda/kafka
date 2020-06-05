package io.confluent.telemetry.provider;

import com.google.common.collect.ImmutableSet;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.opencensus.proto.resource.v1.Resource;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  public static final String RESOURCE_LABEL_CLUSTER_ID = ConfluentConfigs.RESOURCE_LABEL_PREFIX + "cluster.id";
  public static final String KAFKA_CLUSTER_ID = "kafka.cluster.id";
  public static final String KAFKA_BROKER_ID = "kafka.broker.id";
  public static final String CONNECT_KAFKA_CLUSTER_ID = "connect.kafka.cluster.id";
  public static final String CONNECT_GROUP_ID = "connect.group.id";

  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  /**
   * Validate that the map contains the key and the key is a non-empty string
   *
   * @return true if key is valid.
   */
  public static boolean notEmptyString(Map<String, ?> m, String key) {
    if (!m.containsKey(key)) {
      log.trace("{} does not exist in map {}", key, m);
      return false;
    }

    if (m.get(key) == null) {
      log.trace("{} is null. map {}", key, m);
      return false;
    }

    if (!(m.get(key) instanceof String)) {
      log.trace("{} is not a string. map {}", key, m);
      return false;
    }

    String val = (String) m.get(key);

    if (val.isEmpty()) {
      log.trace("{} is empty string. value = {} map {}", key, val, m);
      return false;
    }
    return true;
  }

  /**
   * Extract the resource labels from the metrics context metadata and remove metrics context
   * prefix.
   *
   * @return a map of cleaned up labels.
   */
  public static Map<String, String> getResourceLabels(Map<String, String> metricsCtxMetadata) {
    Set<String> exclude = ImmutableSet.of(MetricsContext.NAMESPACE, KAFKA_CLUSTER_ID, KAFKA_BROKER_ID, CONNECT_KAFKA_CLUSTER_ID, CONNECT_GROUP_ID);
    return metricsCtxMetadata.entrySet().stream()
        .filter(e -> !exclude.contains(e.getKey()))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey()
                    .replace(ConfluentConfigs.RESOURCE_LABEL_PREFIX, ""),
                entry -> entry.getValue())
        );
  }

  /**
   * Build a {@link Resource} from tags in the {@link MetricsContext} metadata.
   * @return
   */
  public static ResourceBuilderFacade buildResourceFromLabels(MetricsContext metricsContext) {
    String clusterId = metricsContext.contextLabels()
        .get(RESOURCE_LABEL_CLUSTER_ID);
    String type = metricsContext.contextLabels().get(ConfluentConfigs.RESOURCE_LABEL_TYPE);
    String version = metricsContext.contextLabels()
        .get(ConfluentConfigs.RESOURCE_LABEL_VERSION);
    ResourceBuilderFacade resourceBuilderFacade = new ResourceBuilderFacade(type.toLowerCase(Locale.ROOT))
        .withVersion(version)
        .withId(clusterId)
        // RESOURCE LABELS (prefixed with resource type)
        .withNamespacedLabels(getResourceLabels(metricsContext.contextLabels()));
    return resourceBuilderFacade;
  }

  public static ResourceBuilderFacade buildResourceFromLabelsAndClusterId(MetricsContext metricsContext, String clusterId) {
    String type = metricsContext.contextLabels().get(ConfluentConfigs.RESOURCE_LABEL_TYPE);
    String version = metricsContext.contextLabels()
            .get(ConfluentConfigs.RESOURCE_LABEL_VERSION);
    ResourceBuilderFacade resourceBuilderFacade = new ResourceBuilderFacade(type.toLowerCase(Locale.ROOT))
            .withVersion(version)
            .withId(clusterId)
            // RESOURCE LABELS (prefixed with resource type)
            .withNamespacedLabels(getResourceLabels(metricsContext.contextLabels()));
    return resourceBuilderFacade;
  }

  /**
   * Validate that the {@link MetricsContext} metadata has tags for - _namespace - type - version -
   * cluster id
   *
   * @return true if valid tags exist, false otherwise.
   */
  public static boolean validateRequiredLabels(Map<String, String> metadata) {
    return notEmptyString(metadata, MetricsContext.NAMESPACE) &&
        notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_TYPE) &&
        notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_VERSION) &&
        notEmptyString(metadata, RESOURCE_LABEL_CLUSTER_ID) &&
        notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID);
  }

  public static boolean validateRequiredResourceLabels(Map<String, String> metadata) {
    return notEmptyString(metadata, MetricsContext.NAMESPACE) &&
            notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_TYPE) &&
            notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_VERSION) &&
            notEmptyString(metadata, ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID);
  }
}
