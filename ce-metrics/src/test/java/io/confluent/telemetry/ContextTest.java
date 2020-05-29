package io.confluent.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.telemetry.reporter.KafkaServerMetricsReporter;
import io.opencensus.proto.resource.v1.Resource;
import org.junit.Test;

public class ContextTest {

  @Test
  public void testDuplicateResourceLabelsOnTimeseries() {
    Resource resource = new ResourceBuilderFacade(KafkaServerMetricsReporter.RESOURCE_TYPE_KAFKA)
        .withLabel("resource_label", "123")
        .withVersion("mockVersion")
        .withId("mockId")
        .build();

    Context context = new Context(resource, "test-domain", false, false);

    assertThat(context.newMetricBuilder().getLabels()).isEmpty();

    context = new Context(resource, "test-domain", false, true);

    assertThat(context.newMetricBuilder().getLabels())
        .isEqualTo(context.getResource().getLabelsMap());

  }
}
