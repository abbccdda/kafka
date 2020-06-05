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
package io.confluent.telemetry.integration;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.KafkaClientProvider;
import io.confluent.telemetry.provider.KafkaConnectProvider;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.resource.v1.Resource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import junit.framework.TestCase;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.integration.ConnectorHandle;
import org.apache.kafka.connect.integration.MonitorableSinkConnector;
import org.apache.kafka.connect.integration.MonitorableSourceConnector;
import org.apache.kafka.connect.integration.RuntimeHandles;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The following test configures and executes up a sink connector pipeline in a worker, produces
 * messages into the source topic-partitions, and demonstrates how to check the overall behavior of
 * the pipeline.
 */
@Category(IntegrationTest.class)
public class KafkaConnectTest extends TelemetryClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(KafkaConnectTest.class);

  private static final int NUM_RECORDS_PRODUCED = 2000;
  private static final int NUM_TOPIC_PARTITIONS = 3;
  private static final long RECORD_TRANSFER_DURATION_MS = TimeUnit.SECONDS.toMillis(30);
  private static final long CONNECTOR_SETUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  private static final int NUM_TASKS = 1;
  private static final int NUM_WORKERS = 1;
  private static final String SOURCE_CONNECTOR_NAME = "simple-source-conn";
  private static final String SINK_CONNECTOR_NAME = "simple-sink-conn";
  private static final String SINK_CONNECTOR_CLASS_NAME = MonitorableSinkConnector.class
      .getSimpleName();
  private static final String SOURCE_CONNECTOR_CLASS_NAME = MonitorableSourceConnector.class
      .getSimpleName();

  private static final String METRICS_CONTEXT_RESOURCE_LABEL_PREFIX = CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_PREFIX;


  private EmbeddedConnectCluster connect;
  private ConnectorHandle sourceConnectorHandle;
  private ConnectorHandle sinkConnectorHandle;

  @Before
  public void setup() throws Exception {
    super.setUp();
    // setup Connect worker properties
    Map<String, String> exampleWorkerProps = new HashMap<>();
    exampleWorkerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(5_000));
    // disable the default exporters
    exampleWorkerProps.put(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");
    exampleWorkerProps.put(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");

    addReporterProps("", exampleWorkerProps);
    addReporterProps("consumer.", exampleWorkerProps);
    addReporterProps("producer.", exampleWorkerProps);


    // setup Kafka broker properties
    Properties exampleBrokerProps = new Properties();
    exampleBrokerProps.put("auto.create.topics.enable", "false");

    // build a Connect cluster backed by Kafka and Zk
    connect = new EmbeddedConnectCluster.Builder()
        .name("connect-cluster")
        .numWorkers(NUM_WORKERS)
        .numBrokers(1)
        .workerProps(exampleWorkerProps)
        .brokerProps(exampleBrokerProps)
        .build();

    // start the clusters
    connect.start();

    // get a handle to the connector
    sinkConnectorHandle = RuntimeHandles.get().connectorHandle(SINK_CONNECTOR_NAME);
    sourceConnectorHandle = RuntimeHandles.get().connectorHandle(SOURCE_CONNECTOR_NAME);

    runSource();
//        runSink();
  }

  private void addReporterProps(String prefix, Map<String, String> props) {
    props.put(prefix + "foo", "bar");
    props.put(prefix + KafkaConfig.MetricReporterClassesProp(),
        "io.confluent.telemetry.reporter.TelemetryReporter");
    props.put(prefix + ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
    props.put(prefix + ConfluentTelemetryConfig.WHITELIST_CONFIG, "");

    props.put(prefix + ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    props.put(prefix + ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
    props.put(prefix + ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.ENABLED_CONFIG, "true");
    props.put(prefix + ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(prefix + ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

    props.put(prefix + "apikey", "foo");
    props.put(prefix + METRICS_CONTEXT_RESOURCE_LABEL_PREFIX  + "region", "test");
    props.put(prefix + METRICS_CONTEXT_RESOURCE_LABEL_PREFIX + "pkc", "pkc-bar");

  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    // delete connector handle
    RuntimeHandles.get().deleteConnector(SINK_CONNECTOR_NAME);
    RuntimeHandles.get().deleteConnector(SOURCE_CONNECTOR_NAME);

    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  /**
   * Simple test case to configure and execute an embedded Connect cluster. The test will produce
   * and consume records, and start up a sink connector which will consume these records.
   */
  public void runSink() throws Exception {
    // create test topic
    connect.kafka().createTopic("test-sink-topic", NUM_TOPIC_PARTITIONS);

    // setup up props for the sink connector
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, SINK_CONNECTOR_CLASS_NAME);
    props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
    props.put(TOPICS_CONFIG, "test-sink-topic");
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    // expect all records to be consumed by the connector
    sinkConnectorHandle.expectedRecords(NUM_RECORDS_PRODUCED);

    // expect all records to be consumed by the connector
    sinkConnectorHandle.expectedCommits(NUM_RECORDS_PRODUCED);

    // validate the intended connector configuration, a config that errors
    connect.assertions()
        .assertExactlyNumErrorsOnConnectorConfigValidation(SINK_CONNECTOR_CLASS_NAME, props, 1,
            "Validating connector configuration produced an unexpected number or errors.");

    // add missing configuration to make the config valid
    props.put("name", SINK_CONNECTOR_NAME);

    // validate the intended connector configuration, a valid config
    connect.assertions()
        .assertExactlyNumErrorsOnConnectorConfigValidation(SINK_CONNECTOR_CLASS_NAME, props, 0,
            "Validating connector configuration produced an unexpected number or errors.");

    // start a sink connector
    connect.configureConnector(SINK_CONNECTOR_NAME, props);

    waitForCondition(this::checkForSinkPartitionAssignment,
        CONNECTOR_SETUP_DURATION_MS,
        "Connector tasks were not assigned a partition each.");

    // produce some messages into source topic partitions
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      connect.kafka()
          .produce("test-topic", i % NUM_TOPIC_PARTITIONS, "key", "simple-message-value-" + i);
    }

    // consume all records from the source topic or fail, to ensure that they were correctly produced.
    assertEquals("Unexpected number of records consumed", NUM_RECORDS_PRODUCED,
        connect.kafka()
            .consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-sink-topic").count());

    // wait for the connector tasks to consume all records.
    sinkConnectorHandle.awaitRecords(RECORD_TRANSFER_DURATION_MS);

    // wait for the connector tasks to commit all records.
    sinkConnectorHandle.awaitCommits(RECORD_TRANSFER_DURATION_MS);

    // delete connector
//        connect.deleteConnector(CONNECTOR_NAME);
  }

  /**
   * Simple test case to configure and execute an embedded Connect cluster. The test will produce
   * and consume records, and start up a sink connector which will consume these records.
   */
  public void runSource() throws Exception {
    // create test topic
    connect.kafka().createTopic("test-source-topic", NUM_TOPIC_PARTITIONS);

    // setup up props for the sink connector
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, SOURCE_CONNECTOR_CLASS_NAME);
    props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
    props.put("topic", "test-source-topic");
    props.put("throughput", String.valueOf(500));
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    // expect all records to be produced by the connector
    sourceConnectorHandle.expectedRecords(NUM_RECORDS_PRODUCED);

    // expect all records to be produced by the connector
    sourceConnectorHandle.expectedCommits(NUM_RECORDS_PRODUCED);

    // validate the intended connector configuration, a config that errors
    connect.assertions()
        .assertExactlyNumErrorsOnConnectorConfigValidation(SOURCE_CONNECTOR_CLASS_NAME, props, 1,
            "Validating connector configuration produced an unexpected number or errors.");

    // add missing configuration to make the config valid
    props.put("name", SOURCE_CONNECTOR_NAME);

    // validate the intended connector configuration, a valid config
    connect.assertions()
        .assertExactlyNumErrorsOnConnectorConfigValidation(SOURCE_CONNECTOR_CLASS_NAME, props, 0,
            "Validating connector configuration produced an unexpected number or errors.");

    // start a source connector
    connect.configureConnector(SOURCE_CONNECTOR_NAME, props);

    // wait for the connector tasks to produce enough records
    sourceConnectorHandle.awaitRecords(RECORD_TRANSFER_DURATION_MS);

    // wait for the connector tasks to commit enough records
    sourceConnectorHandle.awaitCommits(RECORD_TRANSFER_DURATION_MS);

    // consume all records from the source topic or fail, to ensure that they were correctly produced
    int recordNum = connect.kafka()
        .consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-source-topic").count();
    assertTrue("Not enough records produced by source connector. Expected at least: "
            + NUM_RECORDS_PRODUCED + " + but got " + recordNum,
        recordNum >= NUM_RECORDS_PRODUCED);

    // delete connector
//        connect.deleteConnector(CONNECTOR_NAME);
  }

  /**
   * Check if a partition was assigned to each task. This method swallows exceptions since it is
   * invoked from a {@link org.apache.kafka.test.TestUtils#waitForCondition} that will throw an
   * error if this method continued to return false after the specified duration has elapsed.
   *
   * @return true if each task was assigned a partition each, false if this was not true or an error
   * occurred when executing this operation.
   */
  private boolean checkForSinkPartitionAssignment() {
    try {
      ConnectorStateInfo info = connect.connectorStatus(SINK_CONNECTOR_NAME);
      return info != null && info.tasks().size() == NUM_TASKS
          && sinkConnectorHandle.tasks().stream().allMatch(th -> th.partitionsAssigned() == 1);
    } catch (Exception e) {
      // Log the exception and return that the partitions were not assigned
      log.error("Could not check connector state info.", e);
      return false;
    }
  }

  @Test
  public void testMetricsReporter() {
    long startMs = System.currentTimeMillis();
    boolean clientMetricsPresent = false;
    boolean connectMetricsPresent = false;

    while (System.currentTimeMillis() - startMs < 20000) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> record : records) {

        // Verify that the message de-serializes successfully
        Metric m = null;
        try {
          m = this.serde.deserializer()
              .deserialize(record.topic(), record.headers(), record.value());
        } catch (SerializationException e) {
          fail("failed to deserialize message " + e.getMessage());
        }
        // Verify labels

        // Check the resource labels are present
        Resource resource = m.getResource();

        Map<String, String> resourceLabels = resource.getLabelsMap();

        // Check that the labels from the config are present.
        TestCase
            .assertEquals(resourceLabels.get("connect.region"),
                "test");
        TestCase.assertEquals(resourceLabels.get("connect.pkc"),
            "pkc-bar");
        TestCase.assertEquals(resource.getType(), "connect");

        if (m.getMetricDescriptor().getName().startsWith(KafkaConnectProvider.DOMAIN)) {
          connectMetricsPresent = true;
        }
        if (m.getMetricDescriptor().getName().startsWith(KafkaClientProvider.DOMAIN)) {
          clientMetricsPresent = true;
        }
      }
    }
    assertTrue(clientMetricsPresent);
    assertTrue(connectMetricsPresent);
  }
}
