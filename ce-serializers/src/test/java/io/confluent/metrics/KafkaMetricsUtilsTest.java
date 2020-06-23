/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.metrics.record.ConfluentMetric;
import io.confluent.metrics.YammerMetricsUtils.YammerMetric;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaMetricsUtilsTest {

  @Test
  public void testKafkaMetricsWrapperWithTags() {

    Map<String, String> kafkaMetricTags = new HashMap<>();
    kafkaMetricTags.put("key", "val");

    final ConfluentMetric.KafkaMetricName metricName = ConfluentMetric.KafkaMetricName.newBuilder().
            setGroup("TierFetcher").
            setName("BytesFetchedRate").
            putAllTags(kafkaMetricTags).
            build();

    final List<YammerMetric> yammerMetrics = Lists.newArrayList(KafkaMetricsUtils.kafkaMetricsWrapperIterable(
            ImmutableList.of(
                    ConfluentMetric.KafkaMeasurable.newBuilder().setMetricName(metricName).setValue(200.0).build()
            )
    ));

    Map<String, String> yammerTags = yammerMetrics.get(0).getTags();
    assertEquals("KafkaMetric Tags didn't match to YammerMetricsList tags", kafkaMetricTags, yammerTags);
    assertEquals("KafkaMetric name didn't match to YammerMetricsList name", metricName.getName(), yammerMetrics.get(0).getName());
    assertEquals(200, yammerMetrics.get(0).longAggregate(), 0);
    assertEquals(200.0, yammerMetrics.get(0).doubleAggregate(), 0);
  }

  @Test
  public void testKafkaMetricsWrapperNoTags() {

    final ConfluentMetric.KafkaMetricName metricName = ConfluentMetric.KafkaMetricName.newBuilder().
            setGroup("TierFetcher").
            setName("BytesFetchedRate").
            build();

    final List<YammerMetric> yammerMetrics = Lists.newArrayList(KafkaMetricsUtils.kafkaMetricsWrapperIterable(
            ImmutableList.of(
                    ConfluentMetric.KafkaMeasurable.newBuilder().setMetricName(metricName).setValue(200.0).build()
            )
    ));

    Map<String, String> yammerTags = yammerMetrics.get(0).getTags();
    assertTrue(yammerTags.isEmpty());
    assertEquals("KafkaMetric name didn't match to YammerMetricsList name", metricName.getName(), yammerMetrics.get(0).getName());
    assertEquals(200, yammerMetrics.get(0).longAggregate(), 0);
    assertEquals(200.0, yammerMetrics.get(0).doubleAggregate(), 0);
  }
}
