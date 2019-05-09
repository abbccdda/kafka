/**
 * Copyright 2015 Confluent Inc.
 *
 * All rights reserved.
 */
package io.confluent.support.metrics.collectors;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerConfigurationFilterTest {


  @Test
  public void removesSensitiveProperties() {
    // Given/When
    BrokerConfigurationFilter f = new BrokerConfigurationFilter();

    // Then
    assertThat(f.getKeys()).containsOnly(
        "advertised.host.name",
        "advertised.listeners",
        "host.name",
        "listeners",
        "log.dirs",
        "zookeeper.connect",
        "ssl.keystore.location",
        "ssl.keystore.password",
        "ssl.key.password",
        "ssl.truststore.location",
        "ssl.truststore.password"
    );

  }

}