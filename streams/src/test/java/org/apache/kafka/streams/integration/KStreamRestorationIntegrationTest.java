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
package org.apache.kafka.streams.integration;

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


@Category({IntegrationTest.class})
public class KStreamRestorationIntegrationTest {

    private Logger LOG = LoggerFactory.getLogger(KStreamRestorationIntegrationTest.class);

    private StreamsBuilder builder = new StreamsBuilder();

    private KStream<Integer, Integer> transformedStream;

    private static final String APPLICATION_ID = "restoration-test-app";
    private static final String STATE_STORE_NAME = "stateStore";
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    private static final String STATE_STORE_CHANGELOG = APPLICATION_ID + "-" + STATE_STORE_NAME + "-changelog";

    private Properties streamsConfiguration;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private final MockTime mockTime = CLUSTER.time;

    @Before
    public void setUp() throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                APPLICATION_ID,
                CLUSTER.bootstrapServers(),
                Serdes.Integer().getClass().getName(),
                Serdes.Integer().getClass().getName(),
                props);

        CLUSTER.createTopics(INPUT_TOPIC);
        CLUSTER.createTopics("output");
        CLUSTER.createTopics(STATE_STORE_CHANGELOG);

        final StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore(STATE_STORE_NAME),
                        Serdes.Integer(),
                        Serdes.Integer());
        builder.addStateStore(keyValueStoreBuilder);
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }


    @Test
    public void shouldRestoreNullRecord() throws InterruptedException, ExecutionException {
        final KStream<Integer, Integer> sourceStream = builder.stream(INPUT_TOPIC);
        transformedStream = sourceStream.transform(() -> new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {
            private KeyValueStore<Integer, Integer> state;

            @SuppressWarnings("unchecked")
            @Override
            public void init(final ProcessorContext context) {
                state = (KeyValueStore<Integer, Integer>) context.getStateStore(STATE_STORE_NAME);
            }

            @Override
            public KeyValue<Integer, Integer> transform(final Integer key, final Integer value) {
                state.putIfAbsent(key, 0);
                Integer storedValue = state.get(key);
                final KeyValue<Integer, Integer> result = new KeyValue<>(key, value);
                state.put(key, storedValue);
                LOG.info("processed record {}", value);
                return result;
            }

            @Override
            public void close() {
            }
        }, STATE_STORE_NAME);
        transformedStream.to(OUTPUT_TOPIC);

        final List<KeyValue<Integer, Integer>> expectedCountKeyValues = Arrays.asList(KeyValue.pair(1, 1), KeyValue.pair(2, 2), KeyValue.pair(3, null));
        Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class, IntegerSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, expectedCountKeyValues, producerConfig, mockTime);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
        kafkaStreams.start();

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), IntegerDeserializer.class, IntegerDeserializer.class);
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, expectedCountKeyValues);


        kafkaStreams.close();
        kafkaStreams.cleanUp();

//        IntegrationTestUtils.produceKeyValuesSynchronously(STATE_STORE_CHANGELOG, Arrays.asList(KeyValue.pair(3, null)), producerConfig, mockTime);

        // Restart the stream instance
        final List<KeyValue<Integer, Integer>> expectedNewKeyValues = Arrays.asList(KeyValue.pair(1, 1));
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, expectedNewKeyValues, producerConfig, mockTime);
        kafkaStreams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
        kafkaStreams.start();
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, expectedNewKeyValues);
        kafkaStreams.close();
    }
}
