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

package org.apache.kafka.jmh.fetcher;

import java.util.Optional;
import java.util.Properties;
import kafka.cluster.BrokerEndPoint;
import kafka.cluster.Partition;
import kafka.log.LogAppendInfo;
import kafka.server.BlockingSend;
import kafka.server.FailedPartitions;
import kafka.server.KafkaConfig;
import kafka.server.OffsetAndEpoch;
import kafka.server.OffsetTruncationState;
import kafka.server.ReplicaFetcherThread;
import kafka.server.ReplicaManager;
import kafka.server.ReplicaQuota;
import kafka.server.link.ClusterLinkConfig;
import kafka.server.link.ClusterLinkFetcherManager;
import kafka.server.link.ClusterLinkFetcherThread;
import kafka.server.link.ClusterLinkMetadata;
import kafka.server.link.ClusterLinkNetworkClient;
import kafka.utils.Pool;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.Time;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Map;
import scala.compat.java8.OptionConverters;

public class ClusterLinkFetcherThreadBenchmark extends ReplicaFetcherThreadBenchmark {

    @Override
    protected ReplicaFetcherThread createFetcherThread(KafkaConfig config,
                                                       ReplicaManager replicaManager,
                                                       Pool<TopicPartition, Partition> pool) {

        ClusterLinkConfig linkConfig = createClusterLinkConfig();
        ClusterLinkMetadata linkMetadata =  Mockito.mock(ClusterLinkMetadata.class);
        Mockito.when(linkMetadata.linkName()).thenReturn("testLink");
        ClusterLinkFetcherManager linkFetcherManager =  Mockito.mock(ClusterLinkFetcherManager.class);
        ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        Mockito.when(linkFetcherManager.partition(tpCaptor.capture())).thenAnswer(unused ->
            Option.apply(pool.get(tpCaptor.getValue())));
        ClusterLinkNetworkClient linkClient =  Mockito.mock(ClusterLinkNetworkClient.class);
        ReplicaQuota replicaQuota = new ReplicaQuota() {
            @Override
            public boolean isQuotaExceeded() {
                return false;
            }

            @Override
            public void record(long value) {
            }

            @Override
            public boolean isThrottled(TopicPartition topicPartition) {
                return false;
            }
        };
        BlockingSend blockingSend = Mockito.mock(BlockingSend.class);
        return new ClusterLinkFetcherBenchThread(config,
            linkConfig,
            linkMetadata,
            linkFetcherManager,
            linkClient,
            replicaQuota,
            replicaManager,
            blockingSend,
            pool);
    }

    private static ClusterLinkConfig createClusterLinkConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:1234");
        return new ClusterLinkConfig(props);
    }

    static class ClusterLinkFetcherBenchThread extends ClusterLinkFetcherThread {
        private final Pool<TopicPartition, Partition> pool;

        ClusterLinkFetcherBenchThread(KafkaConfig config,
                                      ClusterLinkConfig linkConfig,
                                      ClusterLinkMetadata linkMetadata,
                                      ClusterLinkFetcherManager linkFetcherManager,
                                      ClusterLinkNetworkClient linkClient,
                                      ReplicaQuota replicaQuota,
                                      ReplicaManager replicaManager,
                                      BlockingSend blockingSend,
                                      Pool<TopicPartition, Partition> partitions) {
            super("name",
                3,
                config,
                linkConfig,
                linkMetadata,
                linkFetcherManager,
                new BrokerEndPoint(3, "host", 3000),
                new FailedPartitions(),
                replicaManager,
                replicaQuota,
                new Metrics(),
                Time.SYSTEM,
                Option.empty(),
                linkClient,
                blockingSend,
                Option.empty());

            pool = partitions;
        }

        @Override
        public Option<Object> latestEpoch(TopicPartition topicPartition) {
            return Option.apply(0);
        }

        @Override
        public long logStartOffset(TopicPartition topicPartition) {
            return pool.get(topicPartition).localLogOrException().logStartOffset();
        }

        @Override
        public long logEndOffset(TopicPartition topicPartition) {
            return 0;
        }

        @Override
        public void truncate(TopicPartition tp, OffsetTruncationState offsetTruncationState) {
            // pretend to truncate to move to Fetching state
        }

        @Override
        public Option<OffsetAndEpoch> endOffsetForEpoch(TopicPartition topicPartition, int epoch) {
            return OptionConverters.toScala(Optional.of(new OffsetAndEpoch(0, 0)));
        }

        @Override
        public Option<LogAppendInfo> processPartitionData(TopicPartition topicPartition, long fetchOffset, FetchResponse.PartitionData partitionData) {
            clearPartitionLinkFailure(topicPartition, fetchOffset);
            return Option.empty();
        }

        @Override
        public long fetchEarliestOffsetFromLeader(TopicPartition topicPartition, int currentLeaderEpoch) {
            return 0;
        }

        @Override
        public Map<TopicPartition, EpochEndOffset> fetchEpochEndOffsets(Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> partitions) {
            scala.collection.mutable.Map<TopicPartition, EpochEndOffset> endOffsets = new scala.collection.mutable.HashMap<>();
            Iterator<TopicPartition> iterator = partitions.keys().iterator();
            while (iterator.hasNext()) {
                endOffsets.put(iterator.next(), new EpochEndOffset(0, 100));
            }
            return endOffsets;
        }

        @Override
        public Map<TopicPartition, FetchResponse.PartitionData<Records>> fetchFromLeader(FetchRequest.Builder fetchRequest) {
            return new scala.collection.mutable.HashMap<>();
        }
    }
}
