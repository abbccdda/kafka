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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.KafkaClientProvider;
import io.confluent.telemetry.provider.KafkaStreamsProvider;
import io.confluent.telemetry.provider.Utils;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.resource.v1.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import junit.framework.TestCase;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class KafkaStreamsTest extends TelemetryClusterTestHarness {

  private static final int NUM_BROKERS = 1;

  @ClassRule
  public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
  private final static String APPLICATION_ID_VALUE = "stream-metrics-test";
  // stores name
  private static final String SESSION_AGGREGATED_STREAM_STORE = "session-aggregated-stream-store";
  // topic names
  private static final String STREAM_INPUT = "STREAM_INPUT";
  private static final String STREAM_OUTPUT_1 = "STREAM_OUTPUT_1";
  private static final String STREAM_OUTPUT_2 = "STREAM_OUTPUT_2";
  private static final String STREAM_OUTPUT_3 = "STREAM_OUTPUT_3";
  private static final String STREAM_OUTPUT_4 = "STREAM_OUTPUT_4";
  private final long timeout = 60000;
  private StreamsBuilder builder;
  private Properties streamsConfiguration;
  private KafkaStreams kafkaStreams;

  @Before
  public void before() throws Exception {
    builder = new StreamsBuilder();
    CLUSTER.createTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3,
        STREAM_OUTPUT_4);
    streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_VALUE);
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration
        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    streamsConfiguration
        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration
        .put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

    streamsConfiguration.setProperty(KafkaConfig.MetricReporterClassesProp(),
        "io.confluent.telemetry.reporter.TelemetryReporter");
    // disable the default exporters
    streamsConfiguration.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");
    streamsConfiguration.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");

    streamsConfiguration.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
    streamsConfiguration.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.ENABLED_CONFIG, "true");
    streamsConfiguration.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    streamsConfiguration.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

    streamsConfiguration.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
    streamsConfiguration.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
    streamsConfiguration.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    streamsConfiguration
        .setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_PREFIX + "region", "test");
    streamsConfiguration
        .setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_PREFIX + "pkc", "pkc-bar");
    streamsConfiguration
            .setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + Utils.RESOURCE_LABEL_CLUSTER_ID, "stream-cluster");

    streamsConfiguration
            .setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_TYPE, "ksql");
    streamsConfiguration
            .setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
    streamsConfiguration
            .setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());

    runApplication();
  }

  @After
  public void after() throws Exception {
    closeApplication();
    CLUSTER.deleteTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3,
        STREAM_OUTPUT_4);
  }

  private void startApplication() throws InterruptedException {
    final Topology topology = builder.build();
    kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

    kafkaStreams.start();
    TestUtils.waitForCondition(
        () -> kafkaStreams.state() == State.RUNNING,
        timeout,
        () -> "Kafka Streams application did not reach state RUNNING in " + timeout + " ms");
  }

  private void produceRecordsForTwoSegments(final Duration segmentInterval) throws Exception {
    final MockTime mockTime = new MockTime(Math.max(segmentInterval.toMillis(), 60_000L));
    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
        STREAM_INPUT,
        Collections.singletonList(new KeyValue<>(1, "A")),
        TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            IntegerSerializer.class,
            StringSerializer.class,
            new Properties()),
        mockTime.milliseconds()
    );
    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
        STREAM_INPUT,
        Collections.singletonList(new KeyValue<>(1, "B")),
        TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            IntegerSerializer.class,
            StringSerializer.class,
            new Properties()),
        mockTime.milliseconds()
    );
  }

  private void waitUntilAllRecordsAreConsumed(final int numberOfExpectedRecords) throws Exception {
    IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
        TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(),
            "consumerApp",
            LongDeserializer.class,
            LongDeserializer.class,
            new Properties()
        ),
        STREAM_OUTPUT_1,
        numberOfExpectedRecords
    );
  }

  private void closeApplication() throws Exception {
    kafkaStreams.close();
    kafkaStreams.cleanUp();
    IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    final long timeout = 60000;
    TestUtils.waitForCondition(
        () -> kafkaStreams.state() == State.NOT_RUNNING,
        timeout,
        () -> "Kafka Streams application did not reach state NOT_RUNNING in " + timeout + " ms");
  }

  private void runApplication() throws Exception {

    final Duration inactivityGap = Duration.ofMillis(50);
    builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
        .groupByKey()
        .windowedBy(SessionWindows.with(inactivityGap).grace(Duration.ZERO))
        .aggregate(() -> 0L,
            (aggKey, newValue, aggValue) -> aggValue,
            (aggKey, leftAggValue, rightAggValue) -> leftAggValue,
            Materialized.<Integer, Long, SessionStore<Bytes, byte[]>>as(
                SESSION_AGGREGATED_STREAM_STORE)
                .withValueSerde(Serdes.Long())
                .withRetention(inactivityGap))
        .toStream()
        .map((key, value) -> KeyValue.pair(value, value))
        .to(STREAM_OUTPUT_1, Produced.with(Serdes.Long(), Serdes.Long()));

    produceRecordsForTwoSegments(inactivityGap);
    startApplication();
    waitUntilAllRecordsAreConsumed(2);
  }

  @Test
  public void testMetricsReporter() {
    long startMs = System.currentTimeMillis();
    boolean clientMetricsPresent = false;
    boolean streamsMetricsPresent = false;

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
            .assertEquals("test", resourceLabels.get("ksql.region"));
        TestCase.assertEquals("pkc-bar", resourceLabels.get("ksql.pkc"));

        TestCase.assertEquals("ksql", resource.getType());

        if (m.getMetricDescriptor().getName().startsWith(KafkaStreamsProvider.DOMAIN)) {
          streamsMetricsPresent = true;
        }
        if (m.getMetricDescriptor().getName().startsWith(KafkaClientProvider.DOMAIN)) {
          clientMetricsPresent = true;
        }
      }
    }
    assertTrue(clientMetricsPresent);
    assertTrue(streamsMetricsPresent);
  }
}