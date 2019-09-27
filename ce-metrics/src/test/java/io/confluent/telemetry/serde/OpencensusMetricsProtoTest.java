package io.confluent.telemetry.serde;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.observability.telemetry.MetricBuilderFacade;
import io.confluent.observability.telemetry.ResourceBuilderFacade;
import io.confluent.observability.telemetry.TelemetryResourceType;
import io.confluent.telemetry.MetricsUtils;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;

public class OpencensusMetricsProtoTest {

  private Metric metric;

  @Before
  public void setUp() throws Exception {
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, ZoneId.systemDefault());
    Point point = Point.newBuilder().setInt64Value(100L).setTimestamp(MetricsUtils.now(clock)).build();
    metric = new MetricBuilderFacade()
        .withResource(new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
            .withVersion("mockVersion")
            .withId("mockId")
            .build()
        )
        .withName("name")
        .withType(Type.CUMULATIVE_INT64)
        .withLabel("label1", "value1")
        .addSinglePointTimeseries(point)
        .build();
  }

  @Test
  public void deserializeNoHeadersMatches() {
    Object result = new OpencensusMetricsProto().deserialize("topic", metric.toByteArray());

    final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});
    Object result2 = new OpencensusMetricsProto().deserialize("topic", headers, metric.toByteArray());

    assertEquals(result, result2);
  }

  @Test
  public void serializeDeserialize() {
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
