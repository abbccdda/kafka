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
package org.apache.kafka.streams.integration.utils;

import java.util.concurrent.atomic.AtomicInteger;
import kafka.api.Request;
import kafka.server.KafkaServer;
import kafka.server.MetadataCache;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.ThreadStateTransitionValidator;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentListener;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Utility functions to make integration testing more convenient.
 */
public class IntegrationTestUtils {

    public static final long DEFAULT_TIMEOUT = 60 * 1000L;
    private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestUtils.class);

    /*
     * Records state transition for StreamThread
     */
    public static class StateListenerStub implements StreamThread.StateListener {
        boolean toPendingShutdownSeen = false;
        @Override
        public void onChange(final Thread thread,
                             final ThreadStateTransitionValidator newState,
                             final ThreadStateTransitionValidator oldState) {
            if (newState == StreamThread.State.PENDING_SHUTDOWN) {
                toPendingShutdownSeen = true;
            }
        }

        public boolean transitToPendingShutdownSeen() {
            return toPendingShutdownSeen;
        }
    }

    /**
     * Gives a test name that is safe to be used in application ids, topic names, etc.
     * The name is safe even for parameterized methods.
     */
    public static String safeUniqueTestName(final Class<?> testClass, final TestName testName) {
        return (testClass.getSimpleName() + testName.getMethodName())
                .replace('.', '_')
                .replace('[', '_')
                .replace(']', '_')
                .replace(' ', '_')
                .replace('=', '_');
    }

    /**
     * Removes local state stores. Useful to reset state in-between integration test runs.
     *
     * @param streamsConfiguration Streams configuration settings
     */
    public static void purgeLocalStreamsState(final Properties streamsConfiguration) throws IOException {
        final String tmpDir = TestUtils.IO_TMP_DIR.getPath();
        final String path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        if (path != null) {
            final File node = Paths.get(path).normalize().toFile();
            // Only purge state when it's under java.io.tmpdir.  This is a safety net to prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith(tmpDir)) {
                Utils.delete(new File(node.getAbsolutePath()));
            }
        }
    }

    public static void cleanStateBeforeTest(final EmbeddedKafkaCluster cluster, final String... topics) {
        cleanStateBeforeTest(cluster, 1, topics);
    }

    public static void cleanStateBeforeTest(final EmbeddedKafkaCluster cluster,
                                            final int partitionCount,
                                            final String... topics) {
        try {
            cluster.deleteAllTopicsAndWait(DEFAULT_TIMEOUT);
            for (final String topic : topics) {
                cluster.createTopic(topic, partitionCount, 1);
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void quietlyCleanStateAfterTest(final EmbeddedKafkaCluster cluster, final KafkaStreams driver) {
        try {
            driver.cleanUp();
            cluster.deleteAllTopicsAndWait(DEFAULT_TIMEOUT);
        } catch (final RuntimeException | InterruptedException e) {
            LOG.warn("Ignoring failure to clean test state", e);
        }
    }

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param time           Timestamp provider
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final Collection<KeyValue<K, V>> records,
                                                            final Properties producerConfig,
                                                            final Time time) {
        produceKeyValuesSynchronously(topic, records, producerConfig, time, false);
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param headers             {@link Headers} of the data records
     * @param time                Timestamp provider
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final Collection<KeyValue<K, V>> records,
                                                            final Properties producerConfig,
                                                            final Headers headers,
                                                            final Time time) {
        produceKeyValuesSynchronously(topic, records, producerConfig, headers, time, false);
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param time                Timestamp provider
     * @param enableTransactions  Send messages in a transaction
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final Collection<KeyValue<K, V>> records,
                                                            final Properties producerConfig,
                                                            final Time time,
                                                            final boolean enableTransactions) {
        produceKeyValuesSynchronously(topic, records, producerConfig, null, time, enableTransactions);
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param headers             {@link Headers} of the data records
     * @param time                Timestamp provider
     * @param enableTransactions  Send messages in a transaction
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final Collection<KeyValue<K, V>> records,
                                                            final Properties producerConfig,
                                                            final Headers headers,
                                                            final Time time,
                                                            final boolean enableTransactions) {
        try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            if (enableTransactions) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            for (final KeyValue<K, V> record : records) {
                producer.send(new ProducerRecord<>(topic, null, time.milliseconds(), record.key, record.value, headers));
                time.sleep(1L);
            }
            if (enableTransactions) {
                producer.commitTransaction();
            }
        }
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param timestamp           Timestamp of the record
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp) {
        produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, timestamp, false);
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param timestamp           Timestamp of the record
     * @param enableTransactions  Send messages in a transaction
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Long timestamp,
                                                                         final boolean enableTransactions) {
        produceKeyValuesSynchronouslyWithTimestamp(topic, records, producerConfig, null, timestamp, enableTransactions);
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param headers             {@link Headers} of the data records
     * @param timestamp           Timestamp of the record
     * @param enableTransactions  Send messages in a transaction
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronouslyWithTimestamp(final String topic,
                                                                         final Collection<KeyValue<K, V>> records,
                                                                         final Properties producerConfig,
                                                                         final Headers headers,
                                                                         final Long timestamp,
                                                                         final boolean enableTransactions) {
        try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            if (enableTransactions) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            for (final KeyValue<K, V> record : records) {
                producer.send(new ProducerRecord<>(topic, null, timestamp, record.key, record.value, headers));
            }
            if (enableTransactions) {
                producer.commitTransaction();
            }
        }
    }

    public static <V, K> void produceSynchronously(final Properties producerConfig,
                                                   final boolean eos,
                                                   final String topic,
                                                   final Optional<Integer> partition,
                                                   final List<KeyValueTimestamp<K, V>> toProduce) {
        try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            if (eos) {
                producer.initTransactions();
                producer.beginTransaction();
            }
            final LinkedList<Future<RecordMetadata>> futures = new LinkedList<>();
            for (final KeyValueTimestamp<K, V> record : toProduce) {
                final Future<RecordMetadata> f = producer.send(
                    new ProducerRecord<>(
                        topic,
                        partition.orElse(null),
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        null
                    )
                );
                futures.add(f);
            }

            if (eos) {
                producer.commitTransaction();
            } else {
                producer.flush();
            }

            for (final Future<RecordMetadata> future : futures) {
                try {
                    future.get();
                } catch (final InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Produce data records and send them synchronously in an aborted transaction; that is, a transaction is started for
     * each data record but not committed.
     *
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param timestamp           Timestamp of the record
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     */
    public static <K, V> void produceAbortedKeyValuesSynchronouslyWithTimestamp(
        final String topic,
        final Collection<KeyValue<K, V>> records,
        final Properties producerConfig,
        final Long timestamp
    ) throws Exception {
        try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            producer.initTransactions();
            for (final KeyValue<K, V> record : records) {
                producer.beginTransaction();
                final Future<RecordMetadata> f = producer
                        .send(new ProducerRecord<>(topic, null, timestamp, record.key, record.value));
                f.get();
                producer.abortTransaction();
            }
        }
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param time                Timestamp provider
     * @param <V>                 Value type of the data records
     */
    public static <V> void produceValuesSynchronously(final String topic,
                                                      final Collection<V> records,
                                                      final Properties producerConfig,
                                                      final Time time) {
        produceValuesSynchronously(topic, records, producerConfig, time, false);
    }

    /**
     * @param topic               Kafka topic to write the data records to
     * @param records             Data records to write to Kafka
     * @param producerConfig      Kafka producer configuration
     * @param time                Timestamp provider
     * @param enableTransactions  Send messages in a transaction
     * @param <V>                 Value type of the data records
     */
    @SuppressWarnings("WeakerAccess")
    public static <V> void produceValuesSynchronously(final String topic,
                                                      final Collection<V> records,
                                                      final Properties producerConfig,
                                                      final Time time,
                                                      final boolean enableTransactions) {
        final Collection<KeyValue<Object, V>> keyedRecords = new ArrayList<>();
        for (final V value : records) {
            final KeyValue<Object, V> kv = new KeyValue<>(null, value);
            keyedRecords.add(kv);
        }
        produceKeyValuesSynchronously(topic, keyedRecords, producerConfig, time, enableTransactions);
    }

    /**
     * Wait for streams to "finish", based on the consumer lag metric.
     *
     * Caveats:
     * - Inputs must be finite, fully loaded, and flushed before this method is called
     * - expectedPartitions is the total number of partitions to watch the lag on, including both input and internal.
     *   It's somewhat ok to get this wrong, as the main failure case would be an immediate return due to the clients
     *   not being initialized, which you can avoid with any non-zero value. But it's probably better to get it right ;)
     */
    public static void waitForCompletion(final KafkaStreams streams,
                                         final int expectedPartitions,
                                         final int timeoutMilliseconds) {
        final long start = System.currentTimeMillis();
        while (true) {
            int lagMetrics = 0;
            double totalLag = 0.0;
            for (final Metric metric : streams.metrics().values()) {
                if (metric.metricName().name().equals("records-lag")) {
                    lagMetrics++;
                    totalLag += ((Number) metric.metricValue()).doubleValue();
                }
            }
            if (lagMetrics >= expectedPartitions && totalLag == 0.0) {
                return;
            }
            if (System.currentTimeMillis() - start >= timeoutMilliseconds) {
                throw new RuntimeException(String.format(
                    "Timed out waiting for completion. lagMetrics=[%s/%s] totalLag=[%s]",
                    lagMetrics, expectedPartitions, totalLag
                ));
            }
        }
    }

    /**
     * Wait until enough data (consumer records) has been consumed.
     *
     * @param consumerConfig      Kafka Consumer configuration
     * @param topic               Kafka topic to consume from
     * @param expectedNumRecords  Minimum number of expected records
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     */
    @SuppressWarnings("WeakerAccess")
    public static <K, V> List<ConsumerRecord<K, V>> waitUntilMinRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final int expectedNumRecords
    ) throws Exception {
        return waitUntilMinRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (consumer records) has been consumed.
     *
     * @param consumerConfig      Kafka Consumer configuration
     * @param topic               Kafka topic to consume from
     * @param expectedNumRecords  Minimum number of expected records
     * @param waitTime            Upper bound of waiting time in milliseconds
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     */
    @SuppressWarnings("WeakerAccess")
    public static <K, V> List<ConsumerRecord<K, V>> waitUntilMinRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final int expectedNumRecords,
        final long waitTime
    ) throws Exception {
        final List<ConsumerRecord<K, V>> accumData = new ArrayList<>();
        final String reason = String.format(
            "Did not receive all %d records from topic %s within %d ms",
            expectedNumRecords,
            topic,
            waitTime
        );
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            retryOnExceptionWithTimeout(waitTime, () -> {
                final List<ConsumerRecord<K, V>> readData =
                    readRecords(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                assertThat(reason, accumData.size(), is(greaterThanOrEqualTo(expectedNumRecords)));
            });
        }
        return accumData;
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig      Kafka Consumer configuration
     * @param topic               Kafka topic to consume from
     * @param expectedNumRecords  Minimum number of expected records
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final int expectedNumRecords
    ) throws Exception {
        return waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound of waiting time in milliseconds
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError    if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final int expectedNumRecords,
        final long waitTime
    ) throws Exception {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();
        final String reason = String.format(
            "Did not receive all %d records from topic %s within %d ms",
            expectedNumRecords,
            topic,
            waitTime
        );
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            retryOnExceptionWithTimeout(waitTime, () -> {
                final List<KeyValue<K, V>> readData =
                    readKeyValues(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                assertThat(reason, accumData.size(), is(greaterThanOrEqualTo(expectedNumRecords)));
            });
        }
        return accumData;
    }

    /**
     * Wait until enough data (timestamped key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound of waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
    public static <K, V> List<KeyValueTimestamp<K, V>> waitUntilMinKeyValueWithTimestampRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final int expectedNumRecords,
        final long waitTime
    ) throws Exception {
        final List<KeyValueTimestamp<K, V>> accumData = new ArrayList<>();
        final String reason = String.format(
            "Did not receive all %d records from topic %s within %d ms",
            expectedNumRecords,
            topic,
            waitTime
        );
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            retryOnExceptionWithTimeout(waitTime, () -> {
                final List<KeyValueTimestamp<K, V>> readData =
                    readKeyValuesWithTimestamp(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                assertThat(reason, accumData.size(), is(greaterThanOrEqualTo(expectedNumRecords)));
            });
        }
        return accumData;
    }

    /**
     * Wait until final key-value mappings have been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedRecords    Expected key-value mappings
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the mappings consumed, or null if no records are consumed
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilFinalKeyValueRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final List<KeyValue<K, V>> expectedRecords
    ) throws Exception {
        return waitUntilFinalKeyValueRecordsReceived(consumerConfig, topic, expectedRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until final key-value mappings have been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedRecords    Expected key-value mappings
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the mappings consumed, or null if no records are consumed
     */
    public static <K, V> List<KeyValueTimestamp<K, V>> waitUntilFinalKeyValueTimestampRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final List<KeyValueTimestamp<K, V>> expectedRecords
    ) throws Exception {
        return waitUntilFinalKeyValueRecordsReceived(consumerConfig, topic, expectedRecords, DEFAULT_TIMEOUT, true);
    }

    /**
     * Wait until final key-value mappings have been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Kafka topic to consume from
     * @param expectedRecords    Expected key-value mappings
     * @param waitTime           Upper bound of waiting time in milliseconds
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return All the mappings consumed, or null if no records are consumed
     */
    @SuppressWarnings("WeakerAccess")
    public static <K, V> List<KeyValue<K, V>> waitUntilFinalKeyValueRecordsReceived(
        final Properties consumerConfig,
        final String topic,
        final List<KeyValue<K, V>> expectedRecords,
        final long waitTime
    ) throws Exception {
        return waitUntilFinalKeyValueRecordsReceived(consumerConfig, topic, expectedRecords, waitTime, false);
    }

    @SuppressWarnings("unchecked")
    private static <K, V, T> List<T> waitUntilFinalKeyValueRecordsReceived(final Properties consumerConfig,
                                                                           final String topic,
                                                                           final List<T> expectedRecords,
                                                                           final long waitTime,
                                                                           final boolean withTimestamp) throws Exception {
        final List<T> accumData = new ArrayList<>();
        try (final Consumer<K, V> consumer = createConsumer(consumerConfig)) {
            final TestCondition valuesRead = () -> {
                final List<T> readData;
                if (withTimestamp) {
                    readData = (List<T>) readKeyValuesWithTimestamp(topic, consumer, waitTime, expectedRecords.size());
                } else {
                    readData = (List<T>) readKeyValues(topic, consumer, waitTime, expectedRecords.size());
                }
                accumData.addAll(readData);

                // filter out all intermediate records we don't want
                final List<T> accumulatedActual = accumData
                    .stream()
                    .filter(expectedRecords::contains)
                    .collect(Collectors.toList());

                // still need to check that for each key, the ordering is expected
                final Map<K, List<T>> finalAccumData = new HashMap<>();
                for (final T kv : accumulatedActual) {
                    finalAccumData.computeIfAbsent(
                        withTimestamp ? ((KeyValueTimestamp<K, V>) kv).key() : ((KeyValue<K, V>) kv).key,
                        key -> new ArrayList<>()).add(kv);
                }
                final Map<K, List<T>> finalExpected = new HashMap<>();
                for (final T kv : expectedRecords) {
                    finalExpected.computeIfAbsent(
                        withTimestamp ? ((KeyValueTimestamp<K, V>) kv).key() : ((KeyValue<K, V>) kv).key,
                        key -> new ArrayList<>()).add(kv);
                }

                // returns true only if the remaining records in both lists are the same and in the same order
                // and the last record received matches the last expected record
                return finalAccumData.equals(finalExpected);

            };
            final String conditionDetails = "Did not receive all " + expectedRecords + " records from topic " + topic;
            TestUtils.waitForCondition(valuesRead, waitTime, conditionDetails);
        }
        return accumData;
    }

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError    if the given wait time elapses
     */
    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords) throws Exception {
        return waitUntilMinValuesRecordsReceived(consumerConfig, topic, expectedNumRecords, DEFAULT_TIMEOUT);
    }

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound of waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError    if the given wait time elapses
     */
    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords,
                                                                final long waitTime) throws Exception {
        final List<V> accumData = new ArrayList<>();
        final String reason = String.format(
            "Did not receive all %d records from topic %s within %d ms",
            expectedNumRecords,
            topic,
            waitTime
        );
        try (final Consumer<Object, V> consumer = createConsumer(consumerConfig)) {
            retryOnExceptionWithTimeout(waitTime, () -> {
                final List<V> readData =
                    readValues(topic, consumer, waitTime, expectedNumRecords);
                accumData.addAll(readData);
                assertThat(reason, accumData.size(), is(greaterThanOrEqualTo(expectedNumRecords)));
            });
        }
        return accumData;
    }

    @SuppressWarnings("WeakerAccess")
    public static void waitForTopicPartitions(final List<KafkaServer> servers,
                                              final List<TopicPartition> partitions,
                                              final long timeout) throws InterruptedException {
        final long end = System.currentTimeMillis() + timeout;
        for (final TopicPartition partition : partitions) {
            final long remaining = end - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new AssertionError("timed out while waiting for partitions to become available. Timeout=" + timeout);
            }
            waitUntilMetadataIsPropagated(servers, partition.topic(), partition.partition(), remaining);
        }
    }

    private static void waitUntilMetadataIsPropagated(final List<KafkaServer> servers,
                                                     final String topic,
                                                     final int partition,
                                                     final long timeout) throws InterruptedException {
        final String baseReason = String.format("Metadata for topic=%s partition=%d was not propagated to all brokers within %d ms. ",
            topic, partition, timeout);

        retryOnExceptionWithTimeout(timeout, () -> {
            final List<KafkaServer> emptyPartitionInfos = new ArrayList<>();
            final List<KafkaServer> invalidBrokerIds = new ArrayList<>();

            for (final KafkaServer server : servers) {
                final MetadataCache metadataCache = server.dataPlaneRequestProcessor().metadataCache();
                final Option<UpdateMetadataPartitionState> partitionInfo =
                    metadataCache.getPartitionInfo(topic, partition);

                if (partitionInfo.isEmpty()) {
                    emptyPartitionInfos.add(server);
                    continue;
                }

                final UpdateMetadataPartitionState metadataPartitionState = partitionInfo.get();
                if (!Request.isValidBrokerId(metadataPartitionState.leader())) {
                    invalidBrokerIds.add(server);
                }
            }

            final String reason = baseReason + ". Brokers without partition info: " + emptyPartitionInfos +
                ". Brokers with invalid broker id for partition leader: " + invalidBrokerIds;
            assertThat(reason, emptyPartitionInfos.isEmpty() && invalidBrokerIds.isEmpty());
        });
    }

    /**
     * Starts the given {@link KafkaStreams} instances and waits for all of them to reach the
     * {@link State#RUNNING} state at the same time. Note that states may change between the time
     * that this method returns and the calling function executes its next statement.<p>
     *
     * When the application is already started use {@link #waitForApplicationState(List, State, Duration)}
     * to wait for instances to reach {@link State#RUNNING} state.
     *
     * @param streamsList the list of streams instances to run.
     * @param timeout the time to wait for the streams to all be in {@link State#RUNNING} state.
     */
    public static void startApplicationAndWaitUntilRunning(final List<KafkaStreams> streamsList,
                                                           final Duration timeout) throws Exception {
        final Lock stateLock = new ReentrantLock();
        final Condition stateUpdate = stateLock.newCondition();
        final Map<KafkaStreams, State> stateMap = new HashMap<>();
        for (final KafkaStreams streams : streamsList) {
            stateMap.put(streams, streams.state());
            final StateListener prevStateListener = getStateListener(streams);
            final StateListener newStateListener = (newState, oldState) -> {
                stateLock.lock();
                try {
                    stateMap.put(streams, newState);
                    if (newState == State.RUNNING) {
                        if (stateMap.values().stream().allMatch(state -> state == State.RUNNING)) {
                            stateUpdate.signalAll();
                        }
                    }
                } finally {
                    stateLock.unlock();
                }
            };

            streams.setStateListener(prevStateListener != null
                ? new CompositeStateListener(prevStateListener, newStateListener)
                : newStateListener);
        }

        for (final KafkaStreams streams : streamsList) {
            streams.start();
        }

        final long expectedEnd = System.currentTimeMillis() + timeout.toMillis();
        stateLock.lock();
        try {
            // We use while true here because we want to run this test at least once, even if the
            // timeout has expired
            while (true) {
                final Map<KafkaStreams, State> nonRunningStreams = new HashMap<>();
                for (final Entry<KafkaStreams, State> entry : stateMap.entrySet()) {
                    if (entry.getValue() != State.RUNNING) {
                        nonRunningStreams.put(entry.getKey(), entry.getValue());
                    }
                }

                if (nonRunningStreams.isEmpty()) {
                    return;
                }

                final long millisRemaining = expectedEnd - System.currentTimeMillis();
                if (millisRemaining <= 0) {
                    fail(
                        "Application did not reach a RUNNING state for all streams instances. " +
                            "Non-running instances: " + nonRunningStreams
                    );
                }

                stateUpdate.await(millisRemaining, TimeUnit.MILLISECONDS);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Waits for the given {@link KafkaStreams} instances to all be in a  {@link State#RUNNING}
     * state. Prefer {@link #startApplicationAndWaitUntilRunning(List, Duration)} when possible
     * because this method uses polling, which can be more error prone and slightly slower.
     *
     * @param streamsList the list of streams instances to run.
     * @param timeout the time to wait for the streams to all be in {@link State#RUNNING} state.
     */
    public static void waitForApplicationState(final List<KafkaStreams> streamsList,
                                               final State state,
                                               final Duration timeout) throws Exception {
        retryOnExceptionWithTimeout(timeout.toMillis(), () -> {
            final Map<KafkaStreams, State> streamsToStates = streamsList
                .stream()
                .collect(Collectors.toMap(stream -> stream, KafkaStreams::state));

            final Map<KafkaStreams, State> wrongStateMap = streamsToStates.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != state)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final String reason = String.format(
                "Expected all streams instances in %s to be %s within %d ms, but the following were not: %s",
                streamsList,
                state,
                timeout.toMillis(),
                wrongStateMap
            );
            assertThat(reason, wrongStateMap.isEmpty());
        });
    }

    private static class ConsumerGroupInactiveCondition implements TestCondition {
        private final Admin adminClient;
        private final String applicationId;

        private ConsumerGroupInactiveCondition(final Admin adminClient,
                                               final String applicationId) {
            this.adminClient = adminClient;
            this.applicationId = applicationId;
        }

        @Override
        public boolean conditionMet() {
            try {
                final ConsumerGroupDescription groupDescription =
                    adminClient.describeConsumerGroups(Collections.singletonList(applicationId))
                        .describedGroups()
                        .get(applicationId)
                        .get();
                return groupDescription.members().isEmpty();
            } catch (final ExecutionException | InterruptedException e) {
                return false;
            }
        }
    }

    public static void waitForEmptyConsumerGroup(final Admin adminClient,
                                                 final String applicationId,
                                                 final long timeoutMs) throws Exception {
        TestUtils.waitForCondition(
            new IntegrationTestUtils.ConsumerGroupInactiveCondition(adminClient, applicationId),
            timeoutMs,
            "Test consumer group " + applicationId + " still active even after waiting " + timeoutMs + " ms."
        );
    }

    private static StateListener getStateListener(final KafkaStreams streams) {
        try {
            final Field field = streams.getClass().getDeclaredField("stateListener");
            field.setAccessible(true);
            return (StateListener) field.get(streams);
        } catch (final IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Failed to get StateListener through reflection", e);
        }
    }

    public static <K, V> void verifyKeyValueTimestamps(final Properties consumerConfig,
                                                       final String topic,
                                                       final List<KeyValueTimestamp<K, V>> expected) {
        final List<ConsumerRecord<K, V>> results;
        try {
            results = waitUntilMinRecordsReceived(consumerConfig, topic, expected.size());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        if (results.size() != expected.size()) {
            throw new AssertionError(printRecords(results) + " != " + expected);
        }
        final Iterator<KeyValueTimestamp<K, V>> expectedIterator = expected.iterator();
        for (final ConsumerRecord<K, V> result : results) {
            final KeyValueTimestamp<K, V> expected1 = expectedIterator.next();
            try {
                compareKeyValueTimestamp(result, expected1.key(), expected1.value(), expected1.timestamp());
            } catch (final AssertionError e) {
                throw new AssertionError(printRecords(results) + " != " + expected, e);
            }
        }
    }

    public static void verifyKeyValueTimestamps(final Properties consumerConfig,
                                                final String topic,
                                                final Set<KeyValueTimestamp<String, Long>> expected) {
        final List<ConsumerRecord<String, Long>> results;
        try {
            results = waitUntilMinRecordsReceived(consumerConfig, topic, expected.size());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        if (results.size() != expected.size()) {
            throw new AssertionError(printRecords(results) + " != " + expected);
        }

        final Set<KeyValueTimestamp<String, Long>> actual =
            results.stream()
                   .map(result -> new KeyValueTimestamp<>(result.key(), result.value(), result.timestamp()))
                   .collect(Collectors.toSet());

        assertThat(actual, equalTo(expected));
    }

    private static <K, V> void compareKeyValueTimestamp(final ConsumerRecord<K, V> record,
                                                        final K expectedKey,
                                                        final V expectedValue,
                                                        final long expectedTimestamp) {
        Objects.requireNonNull(record);
        final K recordKey = record.key();
        final V recordValue = record.value();
        final long recordTimestamp = record.timestamp();
        final AssertionError error = new AssertionError(
            "Expected <" + expectedKey + ", " + expectedValue + "> with timestamp=" + expectedTimestamp +
                " but was <" + recordKey + ", " + recordValue + "> with timestamp=" + recordTimestamp
        );
        if (recordKey != null) {
            if (!recordKey.equals(expectedKey)) {
                throw error;
            }
        } else if (expectedKey != null) {
            throw error;
        }
        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }
        if (recordTimestamp != expectedTimestamp) {
            throw error;
        }
    }

    private static <K, V> String printRecords(final List<ConsumerRecord<K, V>> result) {
        final StringBuilder resultStr = new StringBuilder();
        resultStr.append("[\n");
        for (final ConsumerRecord<?, ?> record : result) {
            resultStr.append("  ").append(record.toString()).append("\n");
        }
        resultStr.append("]");
        return resultStr.toString();
    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumer       Kafka consumer
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    private static <V> List<V> readValues(final String topic,
                                          final Consumer<Object, V> consumer,
                                          final long waitTime,
                                          final int maxMessages) {
        final List<V> returnList = new ArrayList<>();
        final List<KeyValue<Object, V>> kvs = readKeyValues(topic, consumer, waitTime, maxMessages);
        for (final KeyValue<?, V> kv : kvs) {
            returnList.add(kv.value);
        }
        return returnList;
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumer       Kafka consumer
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    private static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic,
                                                             final Consumer<K, V> consumer,
                                                             final long waitTime,
                                                             final int maxMessages) {
        final List<KeyValue<K, V>> consumedValues = new ArrayList<>();
        final List<ConsumerRecord<K, V>> records = readRecords(topic, consumer, waitTime, maxMessages);
        for (final ConsumerRecord<K, V> record : records) {
            consumedValues.add(new KeyValue<>(record.key(), record.value()));
        }
        return consumedValues;
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from
     * are already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumer       Kafka consumer
     * @param waitTime       Maximum wait time in milliseconds
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    private static <K, V> List<KeyValueTimestamp<K, V>> readKeyValuesWithTimestamp(final String topic,
                                                                                   final Consumer<K, V> consumer,
                                                                                   final long waitTime,
                                                                                   final int maxMessages) {
        final List<KeyValueTimestamp<K, V>> consumedValues = new ArrayList<>();
        final List<ConsumerRecord<K, V>> records = readRecords(topic, consumer, waitTime, maxMessages);
        for (final ConsumerRecord<K, V> record : records) {
            consumedValues.add(new KeyValueTimestamp<>(record.key(), record.value(), record.timestamp()));
        }
        return consumedValues;
    }

    private static <K, V> List<ConsumerRecord<K, V>> readRecords(final String topic,
                                                                 final Consumer<K, V> consumer,
                                                                 final long waitTime,
                                                                 final int maxMessages) {
        final List<ConsumerRecord<K, V>> consumerRecords;
        consumer.subscribe(Collections.singletonList(topic));
        final int pollIntervalMs = 100;
        consumerRecords = new ArrayList<>();
        int totalPollTimeMs = 0;
        while (totalPollTimeMs < waitTime &&
            continueConsuming(consumerRecords.size(), maxMessages)) {
            totalPollTimeMs += pollIntervalMs;
            final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollIntervalMs));

            for (final ConsumerRecord<K, V> record : records) {
                consumerRecords.add(record);
            }
        }
        return consumerRecords;
    }

    private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    /**
     * Sets up a {@link KafkaConsumer} from a copy of the given configuration that has
     * {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} set to "earliest" and {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}
     * set to "true" to prevent missing events as well as repeat consumption.
     * @param consumerConfig Consumer configuration
     * @return Consumer
     */
    private static <K, V> KafkaConsumer<K, V> createConsumer(final Properties consumerConfig) {
        final Properties filtered = new Properties();
        filtered.putAll(consumerConfig);
        filtered.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        filtered.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(filtered);
    }

    public static KafkaStreams getStartedStreams(final Properties streamsConfig,
                                                 final StreamsBuilder builder,
                                                 final boolean clean) {
        final KafkaStreams driver = new KafkaStreams(builder.build(), streamsConfig);
        if (clean) {
            driver.cleanUp();
        }
        driver.start();
        return driver;
    }

    public static <S> S getStore(final String storeName,
                                 final KafkaStreams streams,
                                 final QueryableStoreType<S> storeType) throws Exception {
        return getStore(DEFAULT_TIMEOUT, storeName, streams, storeType);
    }

    public static <S> S getStore(final String storeName,
                                 final KafkaStreams streams,
                                 final boolean enableStaleQuery,
                                 final QueryableStoreType<S> storeType) throws Exception {
        return getStore(DEFAULT_TIMEOUT, storeName, streams, enableStaleQuery, storeType);
    }

    public static <S> S getStore(final long waitTime,
                                 final String storeName,
                                 final KafkaStreams streams,
                                 final QueryableStoreType<S> storeType) throws Exception {
        return getStore(waitTime, storeName, streams, false, storeType);
    }

    public static <S> S getStore(final long waitTime,
                                 final String storeName,
                                 final KafkaStreams streams,
                                 final boolean enableStaleQuery,
                                 final QueryableStoreType<S> storeType) throws Exception {
        final StoreQueryParameters<S> param = enableStaleQuery ?
            StoreQueryParameters.fromNameAndType(storeName, storeType).enableStaleStores() :
            StoreQueryParameters.fromNameAndType(storeName, storeType);
        return getStore(waitTime, streams, param);
    }

    public static <S> S getStore(final KafkaStreams streams,
                                 final StoreQueryParameters<S> param) throws Exception {
        return getStore(DEFAULT_TIMEOUT, streams, param);
    }

    public static <S> S getStore(final long waitTime,
                                 final KafkaStreams streams,
                                 final StoreQueryParameters<S> param) throws Exception {
        final long expectedEnd = System.currentTimeMillis() + waitTime;
        while (true) {
            try {
                return streams.store(param);
            } catch (final InvalidStateStoreException e) {
                if (System.currentTimeMillis() > expectedEnd) {
                    throw e;
                }
            } catch (final Exception e) {
                if (System.currentTimeMillis() > expectedEnd) {
                    throw new AssertionError(e);
                }
            }
            Thread.sleep(Math.min(100L, waitTime));
        }
    }

    public static class StableAssignmentListener implements AssignmentListener {
        final AtomicInteger numStableAssignments = new AtomicInteger(0);
        int nextExpectedNumStableAssignments;

        @Override
        public void onAssignmentComplete(final boolean stable) {
            if (stable) {
                numStableAssignments.incrementAndGet();
            }
        }

        public int numStableAssignments() {
            return numStableAssignments.get();
        }

        /**
         * Saves the current number of stable rebalances so that we can tell when the next stable assignment has been
         * reached. This should be called once for every invocation of {@link #waitForNextStableAssignment(long)},
         * before the rebalance-triggering event.
         */
        public void prepareForRebalance() {
            nextExpectedNumStableAssignments = numStableAssignments.get() + 1;
        }

        /**
         * Waits for the assignment to stabilize after the group rebalances. You must call {@link #prepareForRebalance()}
         * prior to the rebalance-triggering event before using this method to wait.
         */
        public void waitForNextStableAssignment(final long maxWaitMs) throws InterruptedException {
            waitForCondition(
                () -> nextExpectedNumStableAssignments == numStableAssignments(),
                maxWaitMs,
                () -> "Client did not reach " + nextExpectedNumStableAssignments + " stable assignments on time, " +
                    "numStableAssignments was " + numStableAssignments()
            );
        }
    }
}
