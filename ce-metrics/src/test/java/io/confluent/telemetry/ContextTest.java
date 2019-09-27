package io.confluent.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.observability.telemetry.ResourceBuilderFacade;
import io.confluent.observability.telemetry.TelemetryResourceType;
import io.opencensus.proto.resource.v1.Resource;
import org.junit.Test;

public class ContextTest {

  @Test
  public void testDuplicateResourceLabelsOnTimeseries() {
    Resource resource = new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
        .withLabel("resource_label", "123")
        .withVersion("mockVersion")
        .withId("mockId")
        .build();

    Context context = new Context(resource, false, false);

    assertThat(context.newMetricBuilder().getLabels()).isEmpty();

    context = new Context(resource, false, true);

    assertThat(context.newMetricBuilder().getLabels())
        .isEqualTo(context.getResource().getLabelsMap());

  }
}
