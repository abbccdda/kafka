package io.confluent.telemetry.serde;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.telemetry.MetricsUtils;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class OpencensusMetricsProtoTest {

  final Map<String, String> labels = Collections.singletonMap("label1", "value1");

  @Test
  public void deserializeNoHeadersMatches() {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setInt64Value(100L).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = MetricsUtils
        .metricWithSinglePointTimeseries("name", Type.CUMULATIVE_INT64, labels, point);

    Object result = new OpencensusMetricsProto().deserialize("topic", metric.toByteArray());

    final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});
    Object result2 = new OpencensusMetricsProto().deserialize("topic", headers, metric.toByteArray());

    assertEquals(result, result2);
  }

  @Test
  public void serializeDeserialize() {

    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setInt64Value(100L).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = MetricsUtils
        .metricWithSinglePointTimeseries("name", Type.CUMULATIVE_INT64, labels, point);

    byte[] result = new OpencensusMetricsProto().serialize("topic", metric);

    Metric result2 = new OpencensusMetricsProto().deserialize("topic", result);

    assertEquals(metric, result2);
  }


  @Test
  public void deserializeNull() {
    assertNull(new OpencensusMetricsProto().deserialize("topic", null));
  }

  @Test
  public void badData() {
    SerializationException exception = assertThrows(SerializationException.class, () ->
        new OpencensusMetricsProto().deserialize("topic", new byte[]{0x0, 0x0})
    );

    assertEquals(InvalidProtocolBufferException.class, exception.getCause().getClass());
  }
}