// (Copyright) [2020 - ] Confluent, Inc.

package org.apache.kafka.common;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DelegatingReconfigurable implements Reconfigurable {
  /**
   * A proxy object for Reconfigurables which may not be available at the time the DynamicConfigManager starts.
   *
   */
  private final Supplier<?> supplier;
  protected static final Logger log = LoggerFactory.getLogger(DelegatingReconfigurable.class);

  public DelegatingReconfigurable(Supplier<?> supplier) {
    this.supplier = supplier;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    Object delegate = supplier.get();
    if (delegate instanceof Reconfigurable) {
      return ((Reconfigurable) delegate).reconfigurableConfigs();
    }
    return Collections.emptySet();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    Object delegate = supplier.get();
    if (delegate instanceof Reconfigurable) {
      ((Reconfigurable) delegate).validateReconfiguration(configs);
    }
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    Object delegate = supplier.get();
    if (delegate instanceof Reconfigurable) {
      ((Reconfigurable) delegate).reconfigure(configs);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Object delegate = supplier.get();
    if (delegate instanceof Reconfigurable) {
      ((Reconfigurable) delegate).configure(configs);
    }
  }
}
