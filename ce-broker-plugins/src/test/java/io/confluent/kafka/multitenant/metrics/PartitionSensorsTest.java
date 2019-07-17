// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.multitenant.metrics.TenantMetrics.MetricsRequestContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PartitionSensorsTest {

  private MockTime time = new MockTime();
  private Metrics metrics;

  @Before
  public void setUp() {
    MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.SECONDS);
    metrics = new Metrics(config, time);
  }

  @After
  public void tearDown() throws Exception {
    this.metrics.close();
  }

  @Test
  public void testTenantThroughputPercentiles() {
    String tenant = "tenant1";
    verifyPartitionThroughputPercentiles(tenant, Optional.of(tenant));
  }

  @Test
  public void testBrokerThroughputPercentiles() {
    String tenant = "tenant1";
    verifyPartitionThroughputPercentiles(tenant, Optional.empty());
  }

  private void verifyPartitionThroughputPercentiles(String tenant, Optional<String> metricsTenant) {
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA", new TenantMetadata(tenant, tenant));
    MetricsRequestContext context = createProduceContext(principal);

    PartitionSensors partitionSensors = new PartitionSensorBuilder(metrics, context).build();

    String topic = tenant + "_topic";
    PercentileMetrics percentiles = new PercentileMetrics(metrics, metricsTenant);

    for (int i = 0; i < 100; i++) {
      partitionSensors.recordStatsIn(new TopicPartition(topic, i), 1000 * i, 1);
    }

    percentiles.assertValues(20500, 43050, 73800, 92250, 92250, 92250);

    for (int i = 0; i < 10; i++) {
      partitionSensors.recordStatsIn(new TopicPartition(topic, i), 1000 * 1000, 1);
    }
    for (int i = 10; i < 15; i++) {
      partitionSensors.recordStatsIn(new TopicPartition(topic, i), 2000 * 1000, 1);
    }
    partitionSensors.recordStatsIn(new TopicPartition(topic, 15), 3000 * 1000, 1);
    percentiles.assertValues(30750, 57400, 73800, 1939300, 2933550, 2933550);

    // Verify that samples are expired
    time.sleep(5000);
    partitionSensors.recordStatsIn(new TopicPartition(topic, 0), 100, 1);
    percentiles.percentiles.forEach(p -> assertEquals("Invalid metric " + p.metricName(), 0.0, (Double) p.metricValue(), 1));

    // Verify p99.9 by adding more samples
    for (int i = 0; i < 1000; i++) {
      partitionSensors.recordStatsIn(new TopicPartition(topic, i), 10000, 1);
    }
    percentiles.percentiles.forEach(p -> assertEquals("Invalid metric " + p.metricName(), 6150, (Double) p.metricValue(), 10));

    partitionSensors.recordStatsIn(new TopicPartition(topic, 999), 1000 * 1000, 1);
    percentiles.assertValues(6150, 6150, 6150, 6150, 6150, 953250);
  }

  private static MetricsRequestContext createProduceContext(MultiTenantPrincipal principal) {
    return new MetricsRequestContext(principal, "client-1", ApiKeys.PRODUCE);
  }

  @Test
  public void testThroughputPercentilesWithMultipleTenants() {
    String tenant1 = "tenant1";
    String tenant2 = "tenant2";
    String tenant1Topic = tenant1 + "_topic";
    String tenant2Topic = tenant2 + "_topic";

    MetricsRequestContext context1 = createProduceContext(new MultiTenantPrincipal("userA", new TenantMetadata(tenant1, tenant1)));
    PartitionSensors partitionSensors1 = new PartitionSensorBuilder(metrics, context1).build();
    MetricsRequestContext context2 = createProduceContext(new MultiTenantPrincipal("userA", new TenantMetadata(tenant2, tenant2)));
    PartitionSensors partitionSensors2 = new PartitionSensorBuilder(metrics, context2).build();

    PercentileMetrics tenant1Percentiles = new PercentileMetrics(metrics, Optional.of(tenant1));
    PercentileMetrics tenant2Percentiles = new PercentileMetrics(metrics, Optional.of(tenant2));
    PercentileMetrics brokerPercentiles = new PercentileMetrics(metrics, Optional.empty());

    for (int i = 0; i < 100; i++) {
      partitionSensors1.recordStatsIn(new TopicPartition(tenant1Topic, i), 1000 * i, 100 * i);
    }
    for (int i = 0; i < 100; i++) {
      partitionSensors2.recordStatsIn(new TopicPartition(tenant2Topic, i), 2000 * i, 200 * i);
    }

    tenant1Percentiles.assertValues(20500, 43050, 73800, 92250, 92250, 92250);
    tenant2Percentiles.assertValues(43050, 92250, 135300, 186550, 186550, 186550);
    brokerPercentiles.assertValues(30750, 57400, 92250, 159900, 186550, 186550);
  }

  @Test
  public void testThroughputWithMultipleClientIds() {
    String clientId1 = "producer-1";
    String clientId2 = "producer-2";

    String tenant = "tenantA";
    String topic = "tenantA_topic-1";
    MetricsRequestContext context1 = new MetricsRequestContext(
        new MultiTenantPrincipal("userA", new TenantMetadata(tenant, tenant)),
        clientId1,
        ApiKeys.PRODUCE);
    PartitionSensors partitionSensors1 = new PartitionSensorBuilder(metrics, context1).build();

    MetricsRequestContext context2 = new MetricsRequestContext(
        new MultiTenantPrincipal("userA", new TenantMetadata(tenant, tenant)),
        clientId2,
        ApiKeys.PRODUCE);
    PartitionSensors partitionSensors2 = new PartitionSensorBuilder(metrics, context2).build();

    for (int i = 0; i < 10; i++) {
      partitionSensors1.recordStatsIn(new TopicPartition(topic, i), 1000 * i, 100 * i);
    }
    for (int i = 0; i < 10; i++) {
      partitionSensors2.recordStatsIn(new TopicPartition(topic, i), 2000 * i, 200 * i);
    }

    for (int i = 0; i < 10; ++i) {
      Map<String, String> tags = new HashMap<>();
      tags.put(TenantMetrics.TENANT_TAG, tenant);
      tags.put(TenantMetrics.CLIENT_ID_TAG, clientId1);
      tags.put(PartitionSensors.TOPIC_TAG, topic);
      tags.put(PartitionSensors.PARTITION_TAG, Integer.toString(i));
      tags.put(JmxReporter.JMX_IGNORE_TAG, "");

      KafkaMetric client1BytesIn =
          metrics.metrics().get(metrics.metricName("partition-bytes-in-total", TenantMetrics.GROUP, tags));
      KafkaMetric client1RecordsIn =
          metrics.metrics().get(metrics.metricName("partition-records-in-total", TenantMetrics.GROUP, tags));

      tags.put(TenantMetrics.CLIENT_ID_TAG, clientId2);
      KafkaMetric client2BytesIn =
          metrics.metrics().get(metrics.metricName("partition-bytes-in-total", TenantMetrics.GROUP, tags));
      KafkaMetric client2RecordsIn =
          metrics.metrics().get(metrics.metricName("partition-records-in-total", TenantMetrics.GROUP, tags));
      assertEquals(i * 1000.0, client1BytesIn.metricValue());
      assertEquals(i * 2000.0, client2BytesIn.metricValue());

      assertEquals(i * 100.0, client1RecordsIn.metricValue());
      assertEquals(i * 200.0, client2RecordsIn.metricValue());
    }
  }


  private class PercentileMetrics {
    final Metric p25;
    final Metric p50;
    final Metric p75;
    final Metric p95;
    final Metric p99;
    final Metric p999;
    final List<Metric> percentiles;

    PercentileMetrics(Metrics metrics, Optional<String> tenant) {
      p25 = metrics.metrics().get(metricName(tenant, "partition-bytes-in-p25"));
      p50 = metrics.metrics().get(metricName(tenant, "partition-bytes-in-p50"));
      p75 = metrics.metrics().get(metricName(tenant, "partition-bytes-in-p75"));
      p95 = metrics.metrics().get(metricName(tenant, "partition-bytes-in-p95"));
      p99 = metrics.metrics().get(metricName(tenant, "partition-bytes-in-p99"));
      p999 = metrics.metrics().get(metricName(tenant, "partition-bytes-in-p99.9"));
      percentiles = Arrays.asList(p25, p50, p75, p95, p99, p999);
    }

    void assertValues(double p25, double p50, double p75, double p95, double p99, double p999) {
      assertEquals(p25, (Double) this.p25.metricValue(), 10);
      assertEquals(p50, (Double) this.p50.metricValue(), 10);
      assertEquals(p75, (Double) this.p75.metricValue(), 10);
      assertEquals(p95, (Double) this.p95.metricValue(), 10);
      assertEquals(p99, (Double) this.p99.metricValue(), 10);
      assertEquals(p999, (Double) this.p999.metricValue(), 10);
    }

    private MetricName metricName(Optional<String> tenant, String name) {
      Map<String, String> tags =
          tenant.map(t -> Collections.singletonMap(TenantMetrics.TENANT_TAG, t))
                .orElse(Collections.emptyMap());
      return metrics.metricName(name, TenantMetrics.GROUP, tags);
    }
  }
}
