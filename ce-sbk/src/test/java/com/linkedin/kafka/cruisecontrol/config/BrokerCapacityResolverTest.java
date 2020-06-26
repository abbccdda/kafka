/*
 * Copyright 2017 LinkedIn Corp., 2020 Confluent Inc.
 *  Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.kafka.common.config.internals.ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_DEFAULT;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link BrokerCapacityResolver}
 */
public class BrokerCapacityResolverTest {
  private final Long DISK_TEST_CAPACITY = 10L * 1024; // 10GB in units of MB
  private final Long NW_IN_TEST_CAPACITY = 5L * 1024 * 1024 * 1024; // 5 GB/s in units of bytes
  private final Long NW_OUT_TEST_CAPACITY = 12L * 1024 * 1024 * 1024; // 12GB/s in units of bytes
  private final double BYTES_PER_KB = BrokerCapacityResolver.BYTES_PER_KB;

  private final Double CONVERTED_NW_IN_TEST_CAPACITY = NW_IN_TEST_CAPACITY / BYTES_PER_KB;
  private final Double CONVERTED_NW_OUT_TEST_CAPACITY = NW_OUT_TEST_CAPACITY / BYTES_PER_KB;

  Properties capacityConfigs;
  private static File tempDir;
  private static String dirPath;
  private static FileStore dirStore;
  private static double dirStoreCapacity;

  private static BrokerCapacityConfigResolver getBrokerCapacityResolver(Properties configs) {

    KafkaCruiseControlConfig kccConfig = new KafkaCruiseControlConfig(configs);
    return kccConfig.getConfiguredInstance(KafkaCruiseControlConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
            BrokerCapacityConfigResolver.class);
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
    capacityConfigs = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    capacityConfigs.setProperty(KafkaCruiseControlConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
            BrokerCapacityResolver.class.getCanonicalName());

    capacityConfigs.setProperty(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_SBK_CONFIG, NW_IN_TEST_CAPACITY.toString());
    capacityConfigs.setProperty(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_SBK_CONFIG, NW_OUT_TEST_CAPACITY.toString());
    capacityConfigs.setProperty(KafkaConfig$.MODULE$.LogDirsProp(), dirPath);
  }

  @Test
  public void testParseFromConfig() {
    BrokerCapacityConfigResolver capacityResolver = getBrokerCapacityResolver(capacityConfigs);
    // Return the default capacity in the absence of broker-specific updates
    assertEquals(CONVERTED_NW_IN_TEST_CAPACITY, capacityResolver.capacityForBroker("", "", 0)
            .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(CONVERTED_NW_IN_TEST_CAPACITY, capacityResolver.capacityForBroker("", "", 2)
            .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(CONVERTED_NW_OUT_TEST_CAPACITY, capacityResolver.capacityForBroker("", "", 2)
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
  public void testParseMinimalConfig() {
    Properties minimalRequiredConfigs = new Properties();
    minimalRequiredConfigs.putAll(capacityConfigs);
    minimalRequiredConfigs.remove(KafkaCruiseControlConfig.NETWORK_IN_CAPACITY_BYTES_CONFIG);
    minimalRequiredConfigs.remove(KafkaCruiseControlConfig.NETWORK_OUT_CAPACITY_BYTES_CONFIG);

    BrokerCapacityConfigResolver capacityResolver = getBrokerCapacityResolver(minimalRequiredConfigs);

    assertEquals((double) BALANCER_NETWORK_IN_CAPACITY_DEFAULT / BYTES_PER_KB,
            capacityResolver.capacityForBroker("", "", 0).capacity().get(Resource.NW_IN),
            0.01);
    assertEquals((double) BALANCER_NETWORK_OUT_CAPACITY_DEFAULT / BYTES_PER_KB,
            capacityResolver.capacityForBroker("", "", 2).capacity().get(Resource.NW_IN),
            0.01);
  }

  @Test
  public void testParseNonNumericConfig() {
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_SBK_CONFIG, "bar");
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));
  }

  @Test
  public void testParseNegativeConfig() {
    Long negativeCapacity = -NW_IN_TEST_CAPACITY;
    capacityConfigs.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_SBK_CONFIG, negativeCapacity.toString());
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));
  }

  @Test
  public void testMissingConfigs() {
    Properties missingLogDirsConfig = new Properties();
    missingLogDirsConfig.putAll(capacityConfigs);

    missingLogDirsConfig.remove(KafkaConfig$.MODULE$.LogDirsProp());
    assertThrows(ConfigException.class, () -> getBrokerCapacityResolver(missingLogDirsConfig));
  }

  @Test
  public void testInvalidLogDirsConfig() {
    capacityConfigs.remove(KafkaConfig$.MODULE$.LogDirsProp());
    assertThrows("Expected ConfigException with absent LogDirsProp", ConfigException.class, () -> getBrokerCapacityResolver(capacityConfigs));

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
    assertEquals("Expected network in capacity for broker 2 not to change", CONVERTED_NW_IN_TEST_CAPACITY, networkInCapacity, 0.01);
    assertEquals("Expected network out capacity not to change", CONVERTED_NW_OUT_TEST_CAPACITY, networkOutCapacity, 0.01);

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
