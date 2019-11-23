// (Copyright) [2018 - 2019] Confluent, Inc.

package org.apache.kafka.server.http;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public enum MetadataServers {

  /**
   * Embedded Metadata Server with REST interface.
   */
  CONFLUENT,

  /**
   * Embedded Metadata Service not enabled on the broker.
   */
  NONE;

  public static Set<String> names() {
    return Collections.unmodifiableSet(
        Arrays.stream(values())
            .map(Enum::name)
            .collect(Collectors.toSet()));
  }
}
