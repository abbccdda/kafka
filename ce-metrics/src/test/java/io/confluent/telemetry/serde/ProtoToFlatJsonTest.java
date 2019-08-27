package io.confluent.telemetry.serde;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.confluent.telemetry.MetricBuilderFacade;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.TelemetryResourceType;
import io.opencensus.proto.metrics.v1.DistributionValue;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.SummaryValue;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class ProtoToFlatJsonTest {

  private final MetricBuilderFacade metricBuilder = new MetricBuilderFacade()
      .withResource(new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
          .withVersion("mockVersion")
          .withId("mockId")
          .withLabel("resource_label_1", "resource_value_1")
          .withLabel("resource_label_2", "resource_value_2")
          .build())
      .withLabel("label1", "value1");

  @Test
  public void deserializeNoHeadersMatches() {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setInt64Value(100L).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = metricBuilder.clone()
        .withName("name")
        .withType(Type.CUMULATIVE_INT64)
        .addSinglePointTimeseries(point)
        .build();

    String result = new ProtoToFlatJson().deserialize("topic", metric.toByteArray());
    final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

    String result2 = new ProtoToFlatJson().deserialize("topic", headers, metric.toByteArray());

    assertEquals(result, result2);
  }

  @Test
  public void deserializeCounterDouble() {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setDoubleValue(100d).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = metricBuilder.clone()
        .withName("double_total")
        .withType(Type.CUMULATIVE_DOUBLE)
        .addSinglePointTimeseries(point)
        .build();

    String result = new ProtoToFlatJson(true).deserialize("topic", metric.toByteArray());

    assertEquals(String.format("{\"doubleValue\":100.0,\"kafka_id\":\"mockId\",\"kafka_version\":\"mockVersion\",\"label1\":\"value1\",\"name\":\"double_total\",\"resource_label_1\":\"resource_value_1\",\"resource_label_2\":\"resource_value_2\",\"resource_type\":\"kafka\",\"timestamp\":%s,\"type\":\"CUMULATIVE_DOUBLE\"}", now.toEpochMilli()), result);
  }

  @Test
  public void deserializeGaugeLong() {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setInt64Value(100L).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = metricBuilder.clone()
        .withName("gauge_long_count")
        .withType(Type.GAUGE_INT64)
        .addSinglePointTimeseries(point)
        .build();

    String result = new ProtoToFlatJson(true).deserialize("topic", metric.toByteArray());

    assertEquals(String.format("{\"int64Value\":100,\"kafka_id\":\"mockId\",\"kafka_version\":\"mockVersion\",\"label1\":\"value1\",\"name\":\"gauge_long_count\",\"resource_label_1\":\"resource_value_1\",\"resource_label_2\":\"resource_value_2\",\"resource_type\":\"kafka\",\"timestamp\":%s,\"type\":\"GAUGE_INT64\"}", now.toEpochMilli()), result);
  }

  @Test
  public void deserializeSummary() {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    SummaryValue.Snapshot snapshot = SummaryValue.Snapshot.newBuilder()
        .setSum(DoubleValue.newBuilder().setValue(100d).build())
        .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
            .setPercentile(50.0)
            .setValue(50)
            .build())
        .build();
    SummaryValue summaryValue = SummaryValue.newBuilder()
        .setCount(Int64Value.newBuilder().setValue(2).build())
        .setSnapshot(snapshot)
        .build();
    Point point = Point.newBuilder().setSummaryValue(summaryValue).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = metricBuilder.clone()
        .withName("info_summary")
        .withType(Type.SUMMARY)
        .addSinglePointTimeseries(point)
        .build();

    String result = new ProtoToFlatJson(true).deserialize("topic", metric.toByteArray());

    assertEquals(String.format("{\"50.0\":100.0,\"count\":2,\"kafka_id\":\"mockId\",\"kafka_version\":\"mockVersion\",\"label1\":\"value1\",\"name\":\"info_summary\",\"resource_label_1\":\"resource_value_1\",\"resource_label_2\":\"resource_value_2\",\"resource_type\":\"kafka\",\"sum\":100.0,\"timestamp\":%s,\"type\":\"SUMMARY\"}", now.toEpochMilli()), result);

  }

  @Test
  public void deserializeDistribution() {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    DistributionValue distribution = DistributionValue.newBuilder().setCount(5).setSum(369.73).setSumOfSquaredDeviation(1.0942).build();
    Point point = Point.newBuilder().setDistributionValue(distribution).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = metricBuilder.clone()
        .withName("example_distribution")
        .withType(Type.GAUGE_DISTRIBUTION)
        .addSinglePointTimeseries(point)
        .build();

    String result = new ProtoToFlatJson(true).deserialize("topic", metric.toByteArray());

    assertEquals(String.format("{\"count\":5,\"kafka_id\":\"mockId\",\"kafka_version\":\"mockVersion\",\"label1\":\"value1\",\"name\":\"example_distribution\",\"resource_label_1\":\"resource_value_1\",\"resource_label_2\":\"resource_value_2\",\"resource_type\":\"kafka\",\"sum\":369.73,\"timestamp\":%s,\"type\":\"GAUGE_DISTRIBUTION\",\"variance\":1.0942}", now.toEpochMilli()), result);
  }

  @Test
  public void deserializeWithStartTimestamp() {
    Instant now = Instant.now();
    Instant startInstant = now.minusMillis(1000);
    Timestamp startTs = MetricsUtils.toTimestamp(startInstant);
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setInt64Value(100L).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = metricBuilder.clone()
        .withName("gauge_long_count")
        .withType(Type.GAUGE_INT64)
        .addSinglePointTimeseries(point, startTs)
        .build();

    String result = new ProtoToFlatJson(true).deserialize("topic", metric.toByteArray());

    assertEquals(String.format("{\"int64Value\":100,\"kafka_id\":\"mockId\",\"kafka_version\":\"mockVersion\",\"label1\":\"value1\",\"name\":\"gauge_long_count\",\"resource_label_1\":\"resource_value_1\",\"resource_label_2\":\"resource_value_2\",\"resource_type\":\"kafka\",\"startTimestamp\":%s,\"timestamp\":%s,\"type\":\"GAUGE_INT64\"}", startInstant.toEpochMilli(), now.toEpochMilli()), result);
  }

  @Test
  public void deserializeNull() {
    assertNull(new ProtoToFlatJson().deserialize("topic", null));
  }

  @Test
  public void badData() {
    SerializationException exception = assertThrows(SerializationException.class, () ->
        new ProtoToFlatJson().deserialize("topic", new byte[]{0x0, 0x0})
    );

    assertEquals(InvalidProtocolBufferException.class, exception.getCause().getClass());
  }
}