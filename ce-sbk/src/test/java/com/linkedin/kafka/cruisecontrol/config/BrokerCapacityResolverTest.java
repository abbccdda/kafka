/*
 * Copyright 2017 LinkedIn Corp., 2020 Confluent Inc.
 *  Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link BrokerCapacityResolver}
 */
public class BrokerCapacityResolverTest {
  private final Double DISK_TEST_CAPACITY = 10D * 1024; // 10GB in units of MB
  private final Double NW_IN_TEST_CAPACITY = 5D * 1024 * 1024; // 5 GB/s in units of KB/s
  private final Double NW_OUT_TEST_CAPACITY = 12D * 1024 * 1024; // 12GB/s in units of KB/s
  Map<String, Object> capacityConfigs;
  private static File tempDir;
  private static String dirPath;
  private static FileStore dirStore;
  private static double dirStoreCapacity;

  private static BrokerCapacityConfigResolver getBrokerCapacityResolver(Map<String, Object> configs) {
    BrokerCapacityConfigResolver capacityResolver = new BrokerCapacityResolver();
    capacityResolver.configure(configs);

    return capacityResolver;
  }

  @BeforeClass
  public static void setupDirectories() throws IOException {
    tempDir = TestUtils.tempDirectory();
    dirPath = tempDir.getAbsolutePath();
    dirStore = pathToFileStore(dirPath);
    dirStoreCapacity = (double) dirStore.getTotalSpace() / KafkaCruiseControlUtils.BYTES_IN_MB;
  }

  @Before
  public void setUp() {
    capacityConfigs = new HashMap<>();
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG, NW_IN_TEST_CAPACITY.toString());
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_BASE_CONFIG, NW_OUT_TEST_CAPACITY.toString());
    capacityConfigs.put(KafkaConfig$.MODULE$.LogDirsProp(), dirPath);
  }

  @Test
  public void testParseFromConfig() {
    BrokerCapacityConfigResolver capacityResolver = getBrokerCapacityResolver(capacityConfigs);
    // Return the default capacity in the absence of broker-specific updates
    assertEquals(NW_IN_TEST_CAPACITY, capacityResolver.capacityForBroker("", "", 0)
            .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(NW_IN_TEST_CAPACITY, capacityResolver.capacityForBroker("", "", 2)
            .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(NW_OUT_TEST_CAPACITY, capacityResolver.capacityForBroker("", "", 2)
            .capacity().get(Resource.NW_OUT), 0.01);
    assertEquals(dirStoreCapacity, capacityResolver.capacityForBroker("", "", 2)
            .capacity().get(Resource.DISK), 0.01);

    try {
      capacityResolver.capacityForBroker("", "", BrokerCapacityResolver.DEFAULT_CAPACITY_BROKER_ID);
      fail("Should have thrown exception for negative broker id");
    } catch (IllegalArgumentException e) {
      // let it go
    }
    // We expect all capacities to be estimated, as there's only one source.
    assertTrue(capacityResolver.capacityForBroker("", "", 2).isEstimated());
    assertTrue(capacityResolver.capacityForBroker("", "", 2).estimationInfo().length() > 0);
  }

  @Test
  public void testParseNonNumericConfig() {
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG, "bar");
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));
  }

  @Test
  public void testParseNegativeConfig() {
    Double negativeCapacity = -NW_IN_TEST_CAPACITY;
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG, negativeCapacity.toString());
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));
  }

  @Test
  public void testMissingConfigs() {
    Map<String, Object> missingLogDirsConfig = new HashMap<>(capacityConfigs);
    missingLogDirsConfig.remove(KafkaConfig$.MODULE$.LogDirsProp());
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(missingLogDirsConfig));

    Map<String, Object> missingNwInConfig = new HashMap<>(capacityConfigs);
    missingNwInConfig.remove(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_BASE_CONFIG);
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(missingNwInConfig));

    Map<String, Object> missingNwOutConfig = new HashMap<>(capacityConfigs);
    missingNwOutConfig.remove(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_BASE_CONFIG);
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(missingNwOutConfig));
  }

  @Test
  public void testInvalidLogDirsConfig() {
    capacityConfigs.put(KafkaConfig$.MODULE$.LogDirsProp(), null);
    assertThrows("Expected ConfigException with null LogDirsProp", ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));

    capacityConfigs.put(KafkaConfig$.MODULE$.LogDirsProp(), 1234);
    assertThrows("Expected ConfigException with invalid LogDirsProp", ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));

    capacityConfigs.put(KafkaConfig$.MODULE$.LogDirsProp(), new String[]{"foo"});
    assertThrows("Expected ConfigException with invalid LogDirsProp", ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));
  }

  @Test
  public void testUpdateDiskCapacity() {
    BrokerCapacityConfigResolver capacityResolver = getBrokerCapacityResolver(capacityConfigs);
    BrokerCapacityInfo originalCapacity = capacityResolver.capacityForBroker("", "", 2);
    double originalDiskCapacity = originalCapacity.capacity().get(Resource.DISK);
    assertEquals("Expected disk capacity for broker 2 to be default before update", dirStoreCapacity, originalDiskCapacity, 0.01);
    capacityResolver.updateDiskCapacityForBroker("", "", 2, DISK_TEST_CAPACITY);

    BrokerCapacityInfo updatedCapacity = capacityResolver.capacityForBroker("", "", 2);
    double updatedDiskCapacity = updatedCapacity.capacity().get(Resource.DISK);
    double networkInCapacity = updatedCapacity.capacity().get(Resource.NW_IN);
    double networkOutCapacity = updatedCapacity.capacity().get(Resource.NW_OUT);
    assertEquals("Expected disk capacity for broker 2 to be updated", DISK_TEST_CAPACITY, updatedDiskCapacity, 0.01);
    assertEquals("Expected network in capacity for broker 2 not to change", NW_IN_TEST_CAPACITY, networkInCapacity, 0.01);
    assertEquals("Expected network out capacity not to change", NW_OUT_TEST_CAPACITY, networkOutCapacity, 0.01);

    BrokerCapacityInfo nonUpdatedCapacity = capacityResolver.capacityForBroker("", "", 1);
    double nonUpdatedDiskCapacity = nonUpdatedCapacity.capacity().get(Resource.DISK);
    assertEquals("Expected disk capacity for broker 1 not to change", dirStoreCapacity, nonUpdatedDiskCapacity, 0.01);

    capacityResolver.updateDiskCapacityForBroker("", "", 2, 2 * DISK_TEST_CAPACITY);
    updatedCapacity = capacityResolver.capacityForBroker("", "", 2);
    updatedDiskCapacity = updatedCapacity.capacity().get(Resource.DISK);
    assertEquals("Expected disk capacity to update through multiple calls", 2 * DISK_TEST_CAPACITY, updatedDiskCapacity, 0.01);
  }

  @Test
  public void testUpdateDiskCapacityInvalidBrokerId() {
    BrokerCapacityConfigResolver capacityResolver = getBrokerCapacityResolver(capacityConfigs);
    BrokerCapacityInfo originalCapacity = capacityResolver.capacityForBroker("", "", 0);

    double defaultDiskCapacity = originalCapacity.capacity().get(Resource.DISK);
    assertEquals("Expected disk capacity to be default before update", dirStoreCapacity, defaultDiskCapacity, 0.01);

    capacityResolver.updateDiskCapacityForBroker("", "", 1, DISK_TEST_CAPACITY);
    double updatedBrokerDiskCapacity = capacityResolver.capacityForBroker("", "", 1).capacity().get(Resource.DISK);
    assertEquals("Expected disk capacity for broker 1 to update", DISK_TEST_CAPACITY, updatedBrokerDiskCapacity, 0.01);
    capacityResolver.updateDiskCapacityForBroker("", "", -1, 2 * DISK_TEST_CAPACITY);

    defaultDiskCapacity = capacityResolver.capacityForBroker("", "", 0).capacity().get(Resource.DISK);
    updatedBrokerDiskCapacity = capacityResolver.capacityForBroker("", "", 1).capacity().get(Resource.DISK);
    assertEquals("Expected default disk capacity not to change with invalid broker update", dirStoreCapacity, defaultDiskCapacity, 0.01);
    assertEquals("Expected broker 1 disk capacity not to change with invalid broker update", DISK_TEST_CAPACITY, updatedBrokerDiskCapacity, 0.01);
  }

  private static FileStore pathToFileStore(String path) throws IOException {
    Path pathObj = Paths.get(path);
    return Files.getFileStore(pathObj);
  }
}
