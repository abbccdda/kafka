/**
 * Copyright 2015 Confluent Inc.
 *
 * All rights reserved.
 */

package io.confluent.support.metrics.collectors;

import java.util.HashSet;
import java.util.Set;

import io.confluent.support.metrics.common.Filter;

public class BrokerConfigurationFilter extends Filter {

  private static final Set<String> KEYS_TO_REMOVE;

  static {
    KEYS_TO_REMOVE = new HashSet<>();
    KEYS_TO_REMOVE.add("advertised.host.name");
    KEYS_TO_REMOVE.add("advertised.listeners");
    KEYS_TO_REMOVE.add("host.name");
    KEYS_TO_REMOVE.add("listeners");
    KEYS_TO_REMOVE.add("log.dirs");
    KEYS_TO_REMOVE.add("zookeeper.connect");
    KEYS_TO_REMOVE.add("ssl.keystore.location");
    KEYS_TO_REMOVE.add("ssl.keystore.password");
    KEYS_TO_REMOVE.add("ssl.key.password");
    KEYS_TO_REMOVE.add("ssl.truststore.location");
    KEYS_TO_REMOVE.add("ssl.truststore.password");
  }

  public BrokerConfigurationFilter() {
    super(KEYS_TO_REMOVE);
  }

}
