package io.confluent.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import io.opencensus.proto.resource.v1.Resource;
import org.junit.Test;

public class ResourceBuilderFacadeTest {

  @Test
  public void testBuild() {
    Resource resource = new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
        .withLabel("label", "value")
        .withId("mockId")
        .withVersion("1.0")
        .build();

    assertThat(resource.getType()).isEqualTo(TelemetryResourceType.KAFKA.toCanonicalString());
    assertThat(resource.getLabelsMap()).containsOnly(
        entry("label", "value"),
        entry("kafka_version", "1.0"),
        entry("kafka_id", "mockId")
    );
  }

  @Test
  public void testLabelRedefinition() {
    Resource resource = new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
        .withLabel("label", "value")
        .withLabel("label", "newValue")
        .withId("mockId")
        .withVersion("1.0")
        .build();

    assertThat(resource.getLabelsMap()).contains(
        entry("label", "value")
    );
  }

  @Test
  public void testValdidate_missingVersion() {
    assertThatThrownBy(new ResourceBuilderFacade(TelemetryResourceType.KAFKA)::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Resource version must be set");
  }

  @Test
  public void testValdidate_missingType() {
    assertThatThrownBy(() -> ResourceBuilderFacade.validate(Resource.getDefaultInstance()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Resource type must be set");
  }

}