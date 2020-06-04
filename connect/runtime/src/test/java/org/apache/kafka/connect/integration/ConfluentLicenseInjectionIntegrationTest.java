/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

/**
 * Integration test for the injection of Confluent license properties into licensed connector configs.
 */
@Category(IntegrationTest.class)
public class ConfluentLicenseInjectionIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConfluentLicenseInjectionIntegrationTest.class);

    private final static int NUM_TASKS = 1;

    private EmbeddedConnectCluster connect;
    private Map<String, String> workerProps = new HashMap<>();
    private Properties brokerProps = new Properties();

    @Before
    public void setup() {
        // setup Kafka broker properties
        brokerProps.put("auto.create.topics.enable", String.valueOf(false));
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    @Test
    public void testInjectingDefaultLicensePropertiesIntoConnectors() throws InterruptedException {
        int numWorkers = 1;
        int numBrokers = 1;
        // Add no license-related properties
        String replFactor = Integer.toString(numBrokers);
        workerProps.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, replFactor);
        workerProps.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, replFactor);
        workerProps.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, replFactor);
        workerProps.put("confluent.topic.replication.factor", replFactor);
        connect = new EmbeddedConnectCluster.Builder().name("connect-cluster-1")
                                                      .workerProps(workerProps)
                                                      .numWorkers(numWorkers)
                                                      .numBrokers(numBrokers)
                                                      .brokerProps(brokerProps)
                                                      .build();

        // Start the Connect cluster
        connect.start();
        connect.assertions().assertExactlyNumBrokersAreUp(numBrokers, "Brokers did not start in time.");
        connect.assertions().assertExactlyNumWorkersAreUp(numWorkers, "Worker did not start in time.");
        log.info("Completed startup of {} Kafka brokers and {} Connect workers", numBrokers, numWorkers);

        // base connector props without any license configuration
        final String licensedConnectorClassName = ConfluentLicensedMonitorableSourceConnector.class.getSimpleName();
        final String unlicensedConnectorClassName = ConfluentUnlicensedMonitorableSourceConnector.class.getSimpleName();

        // fail to start a licensed connector that is missing a required property
        String licensedConnectorName = "licensedConnector";
        Map<String, String> licensedConnectorProps = new HashMap<>();
        licensedConnectorProps.put(NAME_CONFIG, licensedConnectorName);
        licensedConnectorProps.put(CONNECTOR_CLASS_CONFIG, licensedConnectorClassName);
        licensedConnectorProps.put(TASKS_MAX_CONFIG, "1");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(licensedConnectorClassName, licensedConnectorProps, 1,
                "Validating licensed connector configuration produced an unexpected number of errors.");

        // set the missing config (but not any license properties) and then start the connector
        licensedConnectorProps.put("foo", "something");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(licensedConnectorClassName, licensedConnectorProps, 0,
                "Validating licensed connector configuration produced an unexpected error.");
        connect.configureConnector(licensedConnectorName, licensedConnectorProps);
        // validate the intended connector configuration, a config that errors
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(licensedConnectorName, NUM_TASKS,
                "Licensed connector tasks did not start in time.");

        // start an unlicensed connector
        String unlicensedConnectorName = "unlicensedConnector";
        Map<String, String> unlicensedConnectorProps = new HashMap<>();
        unlicensedConnectorProps.put(NAME_CONFIG, unlicensedConnectorName);
        unlicensedConnectorProps.put(CONNECTOR_CLASS_CONFIG, unlicensedConnectorClassName);
        unlicensedConnectorProps.put(TASKS_MAX_CONFIG, "1");
        unlicensedConnectorProps.put("bar", "something else");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(unlicensedConnectorClassName, unlicensedConnectorProps, 0,
                "Validating unlicensed connector configuration produced an unexpected error.");
        connect.configureConnector(unlicensedConnectorName, unlicensedConnectorProps);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(unlicensedConnectorName, NUM_TASKS,
                "Unlicensed connector tasks did not start in time.");

        // try to start a licensed connector but specify all but one of the license properties
        String overriddenConnectorName = "anotherLicensedConnector";
        Map<String, String> overriddenConnectorProps = new HashMap<>();
        overriddenConnectorProps.put(NAME_CONFIG, overriddenConnectorName);
        overriddenConnectorProps.put(CONNECTOR_CLASS_CONFIG, licensedConnectorClassName);
        overriddenConnectorProps.put(TASKS_MAX_CONFIG, "1");
        overriddenConnectorProps.put("foo", "something or other");
        overriddenConnectorProps.put("confluent.license", "magic");
        overriddenConnectorProps.put("confluent.topic", "my-license-topic");
        overriddenConnectorProps.put("confluent.topic.replication.factor", "1");
        // We're missing the bootstrap servers, and this should not be injected and validation should fail
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(licensedConnectorClassName, overriddenConnectorProps, 1,
                "Validating licensed connector configuration produced an unexpected number of errors.");

        // Define the missing property and validation should succeed
        overriddenConnectorProps.put("confluent.topic.bootstrap.servers", "localhost:8083");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(licensedConnectorClassName, overriddenConnectorProps, 0,
                "Validating overridden unlicensed connector configuration produced an unexpected error.");
        connect.configureConnector(overriddenConnectorName, overriddenConnectorProps);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(overriddenConnectorName, NUM_TASKS,
                "Overridden licensed connector tasks did not start in time.");

        // Remove the Connect worker
        log.info("Stopping the Connect worker");
        connect.removeWorker();

        // And restart the worker and have the connectors and tasks restart
        log.info("Starting the Connect worker");
        connect.startConnect();

        // Verify the connectors all started
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(licensedConnectorName, NUM_TASKS,
                "Licensed connector tasks did not restart in time.");
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(unlicensedConnectorName, NUM_TASKS,
                "Unlicensed connector tasks did not restart in time.");
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(overriddenConnectorName, NUM_TASKS,
                "Overridden licensed connector tasks did not restart in time.");
    }


    @Test
    public void testNotInjectingDefaultLicensePropertiesIntoConnectorWhenFeatureIsDisabled() throws InterruptedException {
        int numWorkers = 1;
        int numBrokers = 1;
        // Add no license-related properties
        String replFactor = Integer.toString(numBrokers);
        workerProps.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, replFactor);
        workerProps.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, replFactor);
        workerProps.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, replFactor);
        workerProps.put("confluent.topic.replication.factor", replFactor);
        workerProps.put("confluent.license.inject.into.connectors", "false");
        connect = new EmbeddedConnectCluster.Builder().name("connect-cluster-1")
                                                      .workerProps(workerProps)
                                                      .numWorkers(numWorkers)
                                                      .numBrokers(numBrokers)
                                                      .brokerProps(brokerProps)
                                                      .build();

        // Start the Connect cluster
        connect.start();
        connect.assertions().assertExactlyNumBrokersAreUp(numBrokers, "Brokers did not start in time.");
        connect.assertions().assertExactlyNumWorkersAreUp(numWorkers, "Worker did not start in time.");
        log.info("Completed startup of {} Kafka brokers and {} Connect workers", numBrokers, numWorkers);

        // base connector props without any license configuration
        final String licensedConnectorClassName = ConfluentLicensedMonitorableSourceConnector.class.getSimpleName();
        final String unlicensedConnectorClassName = ConfluentUnlicensedMonitorableSourceConnector.class.getSimpleName();

        // fail to start a licensed connector that has no explicit license information
        String licensedConnectorName = "licensedConnector";
        Map<String, String> licensedConnectorProps = new HashMap<>();
        licensedConnectorProps.put(NAME_CONFIG, licensedConnectorName);
        licensedConnectorProps.put(CONNECTOR_CLASS_CONFIG, licensedConnectorClassName);
        licensedConnectorProps.put(TASKS_MAX_CONFIG, "1");
        licensedConnectorProps.put("foo", "something");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(licensedConnectorClassName, licensedConnectorProps, 4,
                "Validating licensed connector configuration produced an unexpected number of errors.");

        // start an unlicensed connector
        String unlicensedConnectorName = "unlicensedConnector";
        Map<String, String> unlicensedConnectorProps = new HashMap<>();
        unlicensedConnectorProps.put(NAME_CONFIG, unlicensedConnectorName);
        unlicensedConnectorProps.put(CONNECTOR_CLASS_CONFIG, unlicensedConnectorClassName);
        unlicensedConnectorProps.put(TASKS_MAX_CONFIG, "1");
        unlicensedConnectorProps.put("bar", "something else");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(unlicensedConnectorClassName, unlicensedConnectorProps, 0,
                "Validating unlicensed connector configuration produced an unexpected error.");
        connect.configureConnector(unlicensedConnectorName, unlicensedConnectorProps);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(unlicensedConnectorName, NUM_TASKS,
                "Unlicensed connector tasks did not start in time.");

        // try to start a licensed connector but specify all but one of the license properties
        String overriddenConnectorName = "anotherLicensedConnector";
        Map<String, String> overriddenConnectorProps = new HashMap<>();
        overriddenConnectorProps.put(NAME_CONFIG, overriddenConnectorName);
        overriddenConnectorProps.put(CONNECTOR_CLASS_CONFIG, licensedConnectorClassName);
        overriddenConnectorProps.put(TASKS_MAX_CONFIG, "1");
        overriddenConnectorProps.put("foo", "something or other");
        overriddenConnectorProps.put("confluent.license", "magic");
        overriddenConnectorProps.put("confluent.topic", "my-license-topic");
        overriddenConnectorProps.put("confluent.topic.replication.factor", "1");
        // We're missing the bootstrap servers, and this should not be injected and validation should fail
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(licensedConnectorClassName, overriddenConnectorProps, 1,
                "Validating licensed connector configuration produced an unexpected number of errors.");

        // Define the missing property and validation should succeed
        overriddenConnectorProps.put("confluent.topic.bootstrap.servers", "localhost:8083");
        connect.assertions().assertExactlyNumErrorsOnConnectorConfigValidation(licensedConnectorClassName, overriddenConnectorProps, 0,
                "Validating overridden unlicensed connector configuration produced an unexpected error.");
        connect.configureConnector(overriddenConnectorName, overriddenConnectorProps);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(overriddenConnectorName, NUM_TASKS,
                "Overridden licensed connector tasks did not start in time.");

        // Remove the Connect worker
        log.info("Stopping the Connect worker");
        connect.removeWorker();

        // And restart the worker and have the connectors and tasks restart
        log.info("Starting the Connect worker");
        connect.startConnect();

        // Verify the connectors all started
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(unlicensedConnectorName, NUM_TASKS,
                "Unlicensed connector tasks did not restart in time.");
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(overriddenConnectorName, NUM_TASKS,
                "Overridden licensed connector tasks did not restart in time.");
    }
}
