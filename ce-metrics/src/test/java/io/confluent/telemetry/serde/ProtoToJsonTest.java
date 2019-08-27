package io.confluent.telemetry.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.MetricBuilderFacade;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.TelemetryResourceType;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class ProtoToJsonTest {

  @Test
  public void deserializeNoHeadersMatches() {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setInt64Value(100L).setTimestamp(MetricsUtils.now(clock)).build();
    Metric metric = new MetricBuilderFacade()
        .withResource(new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
            .withVersion("mockVersion")
            .withId("mockId")
            .build())
        .withName("name")
        .withType(Type.CUMULATIVE_INT64)
        .withLabels(Collections.singletonMap("label1", "value1"))
        .addSinglePointTimeseries(point)
        .build();

    String result = new ProtoToJson().deserialize("topic", metric.toByteArray());

    final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});
    String result2 = new ProtoToJson().deserialize("topic", headers, metric.toByteArray());

    assertEquals(result, result2);
  }

  @Test
  public void deserializeNull() {
    assertNull(new ProtoToJson().deserialize("topic", null));
  }

  @Test
  public void badData() {
    SerializationException exception = assertThrows(SerializationException.class, () ->
        new ProtoToJson().deserialize("topic", new byte[]{0x0, 0x0})
    );

    assertEquals(InvalidProtocolBufferException.class, exception.getCause().getClass());
  }
}