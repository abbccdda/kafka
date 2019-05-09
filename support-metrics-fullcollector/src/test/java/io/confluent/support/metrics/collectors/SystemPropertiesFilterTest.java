/**
 * Copyright 2015 Confluent Inc.
 *
 * All rights reserved.
 */
package io.confluent.support.metrics.collectors;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemPropertiesFilterTest {


  @Test
  public void removesSensitiveProperties() {
    // Given/When
    SystemPropertiesFilter f = new SystemPropertiesFilter();

    // Then
    assertThat(f.getKeys()).containsOnly(
        "kafka.logs.dir",
        "java.class.path",
        "java.ext.dirs",
        "java.home",
        "java.io.tmpdir",
        "java.library.path",
        "log4j.configuration",
        "sun.boot.class.path",
        "sun.boot.library.path",
        "user.dir",
        "user.home",
        "user.language",
        "user.name",
        "user.timezone",
        "user.country",
        "user.country.format",
        "basedir",
        "ftp.nonProxyHosts",
        "http.nonProxyHosts",
        "surefire.real.class.path",
        "surefire.test.class.path"
    );

  }

}