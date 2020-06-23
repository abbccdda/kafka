/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.metrics;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import io.confluent.metrics.record.ConfluentMetric;

import java.util.Map;

public class KafkaMetricsUtils {

  // This class is a wrapper class around KafkaMeasurable objects, wrapping them as
  // YammerMetrics objects. This allows us to use the existing Metrics Extractor logic
  // and YammerMetrics aggregation topology in C3 to support KafkaMetrics.
  public static class KafkaMetricWrapper implements YammerMetricsUtils.YammerMetric {

    protected final ConfluentMetric.KafkaMeasurable kafkaMeasurable;

    public KafkaMetricWrapper(ConfluentMetric.KafkaMeasurable kafkaMeasurable) {
      this.kafkaMeasurable = kafkaMeasurable;
    }

    public String getName() {
      return kafkaMeasurable.getMetricName().getName();
    }

    public Map<String, String> getTags() {
      return kafkaMeasurable.getMetricName().getTagsMap();
    }

    public long longAggregate() {
      return (long) kafkaMeasurable.getValue();
    }

    public double doubleAggregate() {
      return kafkaMeasurable.getValue();
    }
  }

  public static Iterable<YammerMetricsUtils.YammerMetric> kafkaMetricsWrapperIterable(
          Iterable<? extends Message> list) {
    return FluentIterable.from(list).transformAndConcat(
            new Function<Message, Iterable<? extends YammerMetricsUtils.YammerMetric>>() {
              public Iterable<? extends YammerMetricsUtils.YammerMetric> apply(Message input) {
                if (input == null) {
                  throw new IllegalArgumentException("Invalid null input");
                } else if (input instanceof ConfluentMetric.KafkaMeasurable) {
                  return ImmutableList.of(forMessage((ConfluentMetric.KafkaMeasurable) input));
                } else {
                  throw new IllegalArgumentException("Unknown message type " + input.getClass());
                }
              }
            });
  }

  public static YammerMetricsUtils.YammerMetric forMessage(
          final ConfluentMetric.KafkaMeasurable metric) {
    return new KafkaMetricWrapper(metric);
  }
}
