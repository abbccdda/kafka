// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.utils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;


// Metrics utils to register yammer metrics since we don't have access to the KafkaMetrics instance
public class MetricsUtils {

  private static final MetricsRegistry METRICS_REGISTRY;

  static {
    try {
      // KafkaYammerMetrics is in the runtime classpath, but not the compile classpath so we load
      // it via reflection. The reason why we cannot add a compile path dependency to the `core`
      // module is that ce-auth-providers is a library used by MDS and it does not include the
      // Scala version in its name. To avoid issues during packaging, such modules cannot depend
      // on modules that contain Scala code (like `core`).
      Class<?> cls = Class.forName("kafka.metrics.KafkaYammerMetrics");
      METRICS_REGISTRY = (MetricsRegistry) cls.getMethod("defaultRegistry").invoke(null);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to instantiate KafkaYammerMetrics, make sure it's in the classpath", e);
    }
  }

  public static <T> MetricName newGauge(String group,
      String type,
      String name, Map<String, String> tags,
      Supplier<T> valueSupplier) {
    MetricName metricName = metricName(group, type, name, tags);
    METRICS_REGISTRY.newGauge(metricName, new Gauge<T>() {
      @Override
      public T value() {
        return valueSupplier.get();
      }
    });
    return metricName;
  }

  public static Meter newMeter(MetricName metricName, String eventType) {
    return METRICS_REGISTRY.newMeter(metricName, eventType, TimeUnit.SECONDS);
  }

  public static void removeMetrics(Set<MetricName> metricNames) {
    metricNames.forEach(METRICS_REGISTRY::removeMetric);
  }

  public static long elapsedSeconds(Time time, long timeMs) {
    return timeMs == 0 ? 0 : TimeUnit.MILLISECONDS.toSeconds(time.milliseconds() - timeMs);
  }

  public static MetricName metricName(String group, String type, String name, Map<String, String> tags) {
    String scope = Utils.mkString(tags, "", "", ".", ".");
    String mbeanTags = Utils.mkString(tags, "", "", "=", ",");
    if (!mbeanTags.isEmpty())
      mbeanTags = "," + mbeanTags;
    String mbeanName = String.format("%s:type=%s,name=%s%s", group, type, name, mbeanTags);
    return new MetricName(group, type, name, scope, mbeanName);
  }
}
