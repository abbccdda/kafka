package io.confluent.cruisecontrol.metricsreporter;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Timestamp;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.SummaryValue;
import io.opencensus.proto.metrics.v1.TimeSeries;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfluentTelemetryReporterSamplerTest {
  private static final double GAUGE_VALUE = 1000.0;
  private static final long COUNT = 100L;
  private static final double SUM = 40000.0;
  private static final double MEAN_VALUE = 400.0;
  private static final double PERCENTILE_500_VALUE = 650.0;
  private static final double PERCENTILE_999_VALUE = 800.0;
  private static final double MAX_VALUE = 1000.0;
  private static final String TOPIC = "topic";
  private static final String BROKER_STRING = "0";
  private static final int BROKER = 0;
  private static final String PARTITION_STRING = "0";
  private static final int PARTITION = 0;
  private static final Map<String, String> BROKER_LABELS = brokerLabels();
  private static final Map<String, String> TOPIC_LABELS = topicLabels();
  private static final Map<String, String> PARTITION_LABELS = partitionLabels();
  private static final Map<String, String> PRODUCER_REQUEST_LABELS =
          requestLabels(ConfluentTelemetryReporterSampler.PRODUCE_REQUEST_TYPE);
  private static final Map<String, String> CONSUMER_FETCH_REQUEST_LABELS =
          requestLabels(ConfluentTelemetryReporterSampler.CONSUMER_FETCH_REQUEST_TYPE);
  private static final Map<String, String> FOLLOWER_FETCH_REQUEST_LABELS =
          requestLabels(ConfluentTelemetryReporterSampler.FOLLOWER_FETCH_REQUEST_TYPE);
  private static final SummaryValue.Snapshot PERCENTILE_SNAPSHOT = percentileSnapshot();

  private Time time = new MockTime();
  private ConfluentTelemetryReporterSampler sampler = new ConfluentTelemetryReporterSampler();

  @Test
  public void testAllTopicBytesInRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.BYTES_IN_PER_SEC, RawMetricType.ALL_TOPIC_BYTES_IN, true);
  }

  @Test
  public void testTopicBytesInRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.BYTES_IN_PER_SEC, RawMetricType.TOPIC_BYTES_IN, true);
  }

  @Test
  public void testAllTopicBytesOutRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.BYTES_OUT_PER_SEC, RawMetricType.ALL_TOPIC_BYTES_OUT, true);
  }

  @Test
  public void testTopicBytesOutRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.BYTES_OUT_PER_SEC, RawMetricType.TOPIC_BYTES_OUT, true);
  }

  @Test
  public void testAllTopicReplicationBytesInRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REPLICATION_BYTES_IN_PER_SEC, RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN, true);
  }

  @Test
  public void testTopicReplicationBytesInRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REPLICATION_BYTES_IN_PER_SEC, RawMetricType.TOPIC_REPLICATION_BYTES_IN, true);
  }

  @Test
  public void testAllTopicReplicationBytesOutRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REPLICATION_BYTES_OUT_PER_SEC, RawMetricType.ALL_TOPIC_REPLICATION_BYTES_OUT, true);
  }

  @Test
  public void testTopicReplicationBytesOutRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REPLICATION_BYTES_OUT_PER_SEC, RawMetricType.TOPIC_REPLICATION_BYTES_OUT, true);
  }

  @Test
  public void testAllTopicFetchRequestPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.TOTAL_FETCH_REQUEST_PER_SEC, RawMetricType.ALL_TOPIC_FETCH_REQUEST_RATE, true);
  }

  @Test
  public void testTopicFetchRequestPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.TOTAL_FETCH_REQUEST_PER_SEC, RawMetricType.TOPIC_FETCH_REQUEST_RATE, true);
  }

  @Test
  public void testAllTopicProduceRequestPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.TOTAL_PRODUCE_REQUEST_PER_SEC, RawMetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE, true);
  }

  @Test
  public void testTopicProduceRequestPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.TOTAL_PRODUCE_REQUEST_PER_SEC, RawMetricType.TOPIC_PRODUCE_REQUEST_RATE, true);
  }

  @Test
  public void testAllTopicMessagesPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.MESSAGES_IN_PER_SEC, RawMetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC, true);
  }

  @Test
  public void testTopicMessagesPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.MESSAGES_IN_PER_SEC, RawMetricType.TOPIC_MESSAGES_IN_PER_SEC, true);
  }

  @Test
  public void testBrokerProduceRequestPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REQUESTS_PER_SEC, RawMetricType.BROKER_PRODUCE_REQUEST_RATE,
            true, PRODUCER_REQUEST_LABELS);
  }

  @Test
  public void testBrokerConsumerFetchRequestPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REQUESTS_PER_SEC, RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_RATE,
            true, CONSUMER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testBrokerFollowerFetchRequestPerSec() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REQUESTS_PER_SEC, RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_RATE,
            true, FOLLOWER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testRequestQueueSize() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REQUEST_QUEUE_SIZE, RawMetricType.BROKER_REQUEST_QUEUE_SIZE, false);
  }

  @Test
  public void testResponseQueueSize() {
    testGaugeValue(ConfluentTelemetryReporterSampler.RESPONSE_QUEUE_SIZE, RawMetricType.BROKER_RESPONSE_QUEUE_SIZE, false);
  }

  @Test
  public void testProduceRequestQueueTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.REQUEST_QUEUE_TIME_MS, RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH,
            RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH, RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX,
            RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN, PRODUCER_REQUEST_LABELS);
  }

  @Test
  public void testConsumerFetchRequestQueueTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.REQUEST_QUEUE_TIME_MS, RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
            RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
            RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, CONSUMER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testFollowerFetchRequestQueueTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.REQUEST_QUEUE_TIME_MS, RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
            RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
            RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, FOLLOWER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testProduceLocalTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.LOCAL_TIME_MS, RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_50TH,
            RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_999TH, RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MAX,
            RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MEAN, PRODUCER_REQUEST_LABELS);
  }

  @Test
  public void testConsumerFetchLocalTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.LOCAL_TIME_MS, RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH,
            RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH, RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX,
            RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN, CONSUMER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testFollowerFetchLocalTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.LOCAL_TIME_MS, RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH,
            RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH, RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX,
            RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN, FOLLOWER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testProduceTotalTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.TOTAL_TIME_MS, RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_50TH,
            RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_999TH, RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MAX,
            RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MEAN, PRODUCER_REQUEST_LABELS);
  }

  @Test
  public void testConsumerFetchTotalTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.TOTAL_TIME_MS, RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH,
            RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH, RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX,
            RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN, CONSUMER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testFollowerFetchTotalTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.TOTAL_TIME_MS, RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH,
            RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH, RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX,
            RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN, FOLLOWER_FETCH_REQUEST_LABELS);
  }

  @Test
  public void testSize() {
    testGaugeValue(ConfluentTelemetryReporterSampler.SIZE, RawMetricType.PARTITION_SIZE, false);
  }

  @Test
  public void testLogFlushRate() {
    testGaugeValue(ConfluentTelemetryReporterSampler.LOG_FLUSH_RATE, RawMetricType.BROKER_LOG_FLUSH_RATE, true);
  }

  @Test
  public void testLogFlushTime() {
    testSummaryValue(ConfluentTelemetryReporterSampler.LOG_FLUSH_TIME_MS, RawMetricType.BROKER_LOG_FLUSH_TIME_MS_50TH,
            RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH, RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MAX,
            RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MEAN, BROKER_LABELS);
  }

  @Test
  public void testRequestHandlerIdle() {
    testGaugeValue(ConfluentTelemetryReporterSampler.REQUEST_HANDLER_AVG_IDLE_PERCENT,
            RawMetricType.BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT, true);
  }

  @Test
  public void testCpuUsage() {
    testGaugeValue(ConfluentTelemetryReporterSampler.CPU_USAGE, RawMetricType.BROKER_CPU_UTIL, true);
  }

  @Test
  public void testDiskTotalBytes() {
    testGaugeValue(ConfluentTelemetryReporterSampler.DISK_TOTAL_BYTES, RawMetricType.BROKER_DISK_CAPACITY, false);
  }

  private static Map<String, String> brokerLabels() {
    return Utils.mkMap(Utils.mkEntry(ConfluentTelemetryReporterSampler.BROKER_KEY, BROKER_STRING));
  }

  private static Map<String, String> topicLabels() {
    Map<String, String> labels = brokerLabels();
    labels.put(ConfluentTelemetryReporterSampler.TOPIC_KEY, TOPIC);
    return labels;
  }

  private static Map<String, String> partitionLabels() {
    Map<String, String> labels = topicLabels();
    labels.put(ConfluentTelemetryReporterSampler.PARTITION_KEY, PARTITION_STRING);
    return labels;
  }

  private static Map<String, String> requestLabels(String requestType) {
    Map<String, String> labels = brokerLabels();
    labels.put(ConfluentTelemetryReporterSampler.REQUEST_TYPE_KEY, requestType);
    return labels;
  }

  private static SummaryValue.Snapshot percentileSnapshot() {
    return SummaryValue.Snapshot.newBuilder()
            .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(50.0)
                    .setValue(PERCENTILE_500_VALUE)
                    .build())
            .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(99.9)
                    .setValue(PERCENTILE_999_VALUE)
                    .build())
            .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(100.0)
                    .setValue(MAX_VALUE)
                    .build())
            .setCount(Int64Value.newBuilder().setValue(COUNT).build())
            .setSum(DoubleValue.newBuilder().setValue(SUM).build())
            .build();
  }

  private Metric buildMetric(String name, boolean isDouble, Map<String, String> labels, SummaryValue.Snapshot snapshot) {
    Metric.Builder metricBuilder = Metric.newBuilder();

    MetricDescriptor.Builder metricDescriptorBuilder = MetricDescriptor.newBuilder()
            .setName(name);

    Point.Builder pointBuilder = Point.newBuilder()
            .setTimestamp(Timestamp.newBuilder().setSeconds(time.milliseconds() / 1000).build());

    if (snapshot != null) {
      SummaryValue summaryValue = SummaryValue.newBuilder()
              .setSnapshot(snapshot)
              .build();
      pointBuilder.setSummaryValue(summaryValue);
    } else if (isDouble) {
      pointBuilder.setDoubleValue(GAUGE_VALUE);
    } else {
      pointBuilder.setInt64Value((long) GAUGE_VALUE);
    }

    TimeSeries.Builder tsBuilder = TimeSeries.newBuilder().addPoints(pointBuilder.build());
    for (Map.Entry<String, String> label : labels.entrySet()) {
      metricDescriptorBuilder.addLabelKeysBuilder().setKey(label.getKey());
      tsBuilder.addLabelValuesBuilder().setValue(label.getValue());
    }

    return metricBuilder
            .addTimeseries(tsBuilder.build())
            .setMetricDescriptor(metricDescriptorBuilder.build())
            .build();
  }

  private void testGaugeValue(String name, RawMetricType type, boolean isDouble) {
    testGaugeValue(name, type, isDouble, null);
  }

  private void testGaugeValue(String name, RawMetricType type, boolean isDouble, Map<String, String> labels) {
    if (labels == null) {
      switch (type.metricScope()) {
        case BROKER:
          labels = BROKER_LABELS;
          break;
        case TOPIC:
          labels = TOPIC_LABELS;
          break;
        case PARTITION:
          labels = PARTITION_LABELS;
          break;
      }
    }

    Metric metric = buildMetric(name, isDouble, labels, null);
    List<CruiseControlMetric> ccMetrics = sampler.convertMetricRecord(metric.toByteArray());

    assertEquals(1, ccMetrics.size());
    CruiseControlMetric ccMetric = ccMetrics.get(0);
    assertEquals(type, ccMetric.rawMetricType());
    assertEquals(GAUGE_VALUE, ccMetric.value(), 0.0);
    assertEquals(BROKER, ccMetric.brokerId());

    // The synthetic metric only specifies seconds, so zero out the lower digits
    assertEquals((time.milliseconds() / 1000) * 1000, ccMetric.time());

    if (type.metricScope() == RawMetricType.MetricScope.TOPIC) {
      assertEquals(TOPIC, ((TopicMetric) ccMetric).topic());
    }

    if (type.metricScope() == RawMetricType.MetricScope.PARTITION) {
      assertEquals(TOPIC, ((PartitionMetric) ccMetric).topic());
      assertEquals(PARTITION, ((PartitionMetric) ccMetric).partition());
    }
  }

  private void testSummaryValue(String name,
                                RawMetricType percentile500Type,
                                RawMetricType percentile999Type,
                                RawMetricType maxType,
                                RawMetricType meanType,
                                Map<String, String> labels) {
    Metric metric = buildMetric(name, true, labels, PERCENTILE_SNAPSHOT);
    List<CruiseControlMetric> ccMetrics = sampler.convertMetricRecord(metric.toByteArray());

    assertEquals(4, ccMetrics.size());

    assertTrue(ccMetrics.stream().anyMatch(ccMetric -> ccMetric.brokerId() == BROKER &&
            ccMetric.rawMetricType() == percentile500Type && ccMetric.value() == PERCENTILE_500_VALUE));
    assertTrue(ccMetrics.stream().anyMatch(ccMetric -> ccMetric.brokerId() == BROKER &&
            ccMetric.rawMetricType() == percentile999Type && ccMetric.value() == PERCENTILE_999_VALUE));
    assertTrue(ccMetrics.stream().anyMatch(ccMetric -> ccMetric.brokerId() == BROKER &&
            ccMetric.rawMetricType() == maxType && ccMetric.value() == MAX_VALUE));
    assertTrue(ccMetrics.stream().anyMatch(ccMetric -> ccMetric.brokerId() == BROKER &&
            ccMetric.rawMetricType() == meanType && ccMetric.value() == MEAN_VALUE));
  }
}
