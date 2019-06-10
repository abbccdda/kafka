// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.utils;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;


// Metrics utils to register yammer metrics since we don't have access to the KafkaMetrics instance
public class MetricsUtils {

  public static <T> MetricName newGauge(String group,
      String type,
      String name, Map<String, String> tags,
      Supplier<T> valueSupplier) {
    MetricName metricName = metricName(group, type, name, tags);
    Metrics.defaultRegistry().newGauge(metricName, new Gauge<T>() {
      @Override
      public T value() {
        return valueSupplier.get();
      }
    });
    return metricName;
  }

  public static Meter newMeter(MetricName metricName, String eventType) {
    return Metrics.defaultRegistry().newMeter(metricName, eventType, TimeUnit.SECONDS);
  }

  public static void removeMetrics(Set<MetricName> metricNames) {
    metricNames.forEach(Metrics.defaultRegistry()::removeMetric);
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
