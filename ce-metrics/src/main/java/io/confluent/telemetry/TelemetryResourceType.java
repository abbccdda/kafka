package io.confluent.telemetry;

import io.opencensus.proto.resource.v1.Resource;
import java.util.Locale;

/**
 * Valid values for {@link Resource#getType()}
 *
 * See <a href="https://confluentinc.atlassian.net/wiki/spaces/SECENG/pages/921473450/Resource+Identifiers">this reference</a> for canonical type names.
 *
 * TODO Push this into the <code>telemetry-api</code> module once PR is merged
 */
public enum TelemetryResourceType {

  /**
   * Kafka broker
   */
  KAFKA;

  public String toCanonicalString() {
    return name().toLowerCase(Locale.ENGLISH);
  }

}
