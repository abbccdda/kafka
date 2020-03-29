/*
 * Copyright 2017 LinkedIn Corp., 2020 Confluent Inc.
 *  Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link BrokerCapacityFromConfigResolver}
 */
public class BrokerCapacityFromConfigResolverTest {
  private final Long DISK_TEST_CAPACITY = 10L * 1024 * 1024 * 1024; // 10GB
  private final Long NW_IN_TEST_CAPACITY = 5L * 1024 * 1024 * 1024; // 5 GB/s
  private final Long NW_OUT_TEST_CAPACITY = 12L * 1024 * 1024 * 1024; // 12GB/s

  private static BrokerCapacityConfigResolver getBrokerCapacityConfigResolver(Map<String, String> configs) {
    BrokerCapacityConfigResolver configResolver = new BrokerCapacityFromConfigResolver();
    configResolver.configure(configs);

    return configResolver;
  }

  @Test
  public void testParseFromConfig() {
    Map<String, String> capacityConfigs = new HashMap<>();
    capacityConfigs.put(ConfluentConfigs.BALANCER_DISK_CAPACITY_BASE_CONFIG, DISK_TEST_CAPACITY.toString());
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG, NW_IN_TEST_CAPACITY.toString());
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_BASE_CONFIG, NW_OUT_TEST_CAPACITY.toString());
    BrokerCapacityConfigResolver configResolver = getBrokerCapacityConfigResolver(capacityConfigs);

    // There are no specific-to-broker capacities. Always return the default.
    assertEquals(NW_IN_TEST_CAPACITY, configResolver.capacityForBroker("", "", 0)
            .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(NW_IN_TEST_CAPACITY, configResolver.capacityForBroker("", "", 2)
            .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(DISK_TEST_CAPACITY, configResolver.capacityForBroker("", "", 0)
            .capacity().get(Resource.DISK), 0.0);
    assertEquals(NW_OUT_TEST_CAPACITY, configResolver.capacityForBroker("", "", 2)
            .capacity().get(Resource.NW_OUT), 0.01);

    try {
      configResolver.capacityForBroker("", "", BrokerCapacityFromConfigResolver.DEFAULT_CAPACITY_BROKER_ID);
      fail("Should have thrown exception for negative broker id");
    } catch (IllegalArgumentException e) {
      // let it go
    }

    // We expect all capacities to be estimated, as there's only one source.
    assertTrue(configResolver.capacityForBroker("", "", 2).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 2).estimationInfo().length() > 0);
  }

  @Test
  public void testParseNonNumericConfig() {
    Map<String, String> capacityConfigs = new HashMap<>();
    capacityConfigs.put(ConfluentConfigs.BALANCER_DISK_CAPACITY_BASE_CONFIG, "foo");
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG, "bar");
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_BASE_CONFIG, NW_OUT_TEST_CAPACITY.toString());
    assertThrows(ConfigException.class, () -> getBrokerCapacityConfigResolver(capacityConfigs));
  }

  @Test
  public void testParseNegativeConfig() {
    Long badDiskCapacity = -10L * 1024 * 1024 * 1024;
    Map<String, String> capacityConfigs = new HashMap<>();
    capacityConfigs.put(ConfluentConfigs.BALANCER_DISK_CAPACITY_BASE_CONFIG, badDiskCapacity.toString());
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG, "bar");
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_BASE_CONFIG, NW_OUT_TEST_CAPACITY.toString());
    assertThrows(ConfigException.class, () -> getBrokerCapacityConfigResolver(capacityConfigs));
  }

  @Test
  public void testMissingConfig() {
    Map<String, String> capacityConfigs = new HashMap<>();
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG, NW_IN_TEST_CAPACITY.toString());
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_BASE_CONFIG, NW_OUT_TEST_CAPACITY.toString());
    assertThrows(ConfigException.class, () -> getBrokerCapacityConfigResolver(capacityConfigs));
  }
}