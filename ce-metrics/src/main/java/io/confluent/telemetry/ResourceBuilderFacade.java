package io.confluent.telemetry;

import com.google.common.base.Preconditions;
import io.opencensus.proto.resource.v1.Resource;
import io.opencensus.proto.resource.v1.ResourceOrBuilder;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper on the protobuf {@link Resource.Builder} that enforces Confluent conventions.
 *
 * TODO Push this into the <code>telemetry-api</code> module once PR is merged
 */
public class ResourceBuilderFacade {

  private static final Logger log = LoggerFactory.getLogger(ResourceBuilderFacade.class);

  private static final String VERSION_LABEL_SUFFIX = "_version";

  private static final String ID_LABEL_SUFFIX = "_id";

  private final Resource.Builder resourceBuilder;

  public ResourceBuilderFacade(TelemetryResourceType resourceType) {
    Objects.requireNonNull(resourceType, "Resource type must not be null");
    resourceBuilder = Resource.newBuilder().setType(resourceType.toCanonicalString());
  }

  public ResourceBuilderFacade withVersion(String version) {
    Objects.requireNonNull(version, "Version must not be null");
    resourceBuilder.putLabels(resourceBuilder.getType() + VERSION_LABEL_SUFFIX, version);
    return this;
  }

  public ResourceBuilderFacade withId(String id) {
    Objects.requireNonNull(id, "ID must not be null");
    resourceBuilder.putLabels(resourceBuilder.getType() + ID_LABEL_SUFFIX, id);
    return this;
  }

  /**
   * Add a label to this Resource.
   * If the label key already exists, the new value will be ignored and a warning logged.
   */
  public ResourceBuilderFacade withLabel(String key, String value) {
    if (resourceBuilder.containsLabels(key)) {
      log.warn("Ignoring redefinition of existing telemetry label {}", key);
    } else {
      resourceBuilder.putLabels(key, value);
    }
    return this;
  }

  /**
   * Add labels to this Resource.
   * If any label keys already exist, the new value for those labels will be ignored.
   */
  public ResourceBuilderFacade withLabels(Map<String, String> labels) {
    labels.forEach(this::withLabel);
    return this;
  }

  /**
   * Validate a {@link Resource} conforms to Confluent conventions.
   *
   * @param resource A resource <i>potentially not constructed using this builder</i>.
   */
  static void validate(ResourceOrBuilder resource) {
    Preconditions.checkState(!resource.getType().isEmpty(), "Resource type must be set");

    String versionLabel = resource.getType() + VERSION_LABEL_SUFFIX;
    Preconditions.checkState(resource.containsLabels(versionLabel), "Resource version must be set");

    String idLabel = resource.getType() + ID_LABEL_SUFFIX;
    Preconditions.checkState(resource.containsLabels(idLabel), "Resource ID must be set");
  }

  public Resource build() {
    validate(resourceBuilder);
    return resourceBuilder.build();
  }

}
