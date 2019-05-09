/**
 * Copyright 2015 Confluent Inc.
 *
 * All rights reserved.
 */

package io.confluent.support.metrics.collectors;

import java.util.HashSet;
import java.util.Set;

import io.confluent.support.metrics.common.Filter;

public class SystemPropertiesFilter extends Filter {

  private static final Set<String> KEYS_TO_REMOVE;

  static {
    KEYS_TO_REMOVE = new HashSet<>();
    KEYS_TO_REMOVE.add("java.class.path");
    KEYS_TO_REMOVE.add("java.ext.dirs");
    KEYS_TO_REMOVE.add("java.home");
    KEYS_TO_REMOVE.add("java.io.tmpdir");
    KEYS_TO_REMOVE.add("java.library.path");
    KEYS_TO_REMOVE.add("kafka.logs.dir");
    KEYS_TO_REMOVE.add("log4j.configuration");
    KEYS_TO_REMOVE.add("sun.boot.class.path");
    KEYS_TO_REMOVE.add("sun.boot.library.path");
    KEYS_TO_REMOVE.add("user.dir");
    KEYS_TO_REMOVE.add("user.home");
    KEYS_TO_REMOVE.add("user.language");
    KEYS_TO_REMOVE.add("user.name");
    KEYS_TO_REMOVE.add("user.timezone");
    KEYS_TO_REMOVE.add("user.country");
    KEYS_TO_REMOVE.add("user.country.format");
    KEYS_TO_REMOVE.add("basedir");
    KEYS_TO_REMOVE.add("ftp.nonProxyHosts");
    KEYS_TO_REMOVE.add("http.nonProxyHosts");
    KEYS_TO_REMOVE.add("surefire.real.class.path");
    KEYS_TO_REMOVE.add("surefire.test.class.path");

  }

  public SystemPropertiesFilter() {
    super(KEYS_TO_REMOVE);
  }

}
