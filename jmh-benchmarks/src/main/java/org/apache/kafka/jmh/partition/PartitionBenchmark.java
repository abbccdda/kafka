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

package org.apache.kafka.jmh.partition;

import kafka.api.ApiVersion$;
import kafka.cluster.DelayedOperations;
import kafka.cluster.Partition;
import kafka.cluster.PartitionStateStore;
import kafka.log.CleanerConfig;
import kafka.log.Defaults;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.server.BrokerState;
import kafka.server.BrokerTopicStats;
import kafka.server.LogDirFailureChannel;
import kafka.server.LogOffsetMetadata;
import kafka.server.MetadataCache;
import kafka.server.checkpoints.OffsetCheckpoints;
import kafka.tier.TierMetadataManager;
import kafka.tier.state.FileTierPartitionStateFactory;
import kafka.utils.KafkaScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.utils.Time;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PartitionBenchmark {
    @Param({"false", "true"})
    private static String observerFeatureStr;
    private TopicPartition topicPartition = new TopicPartition(UUID.randomUUID().toString(), 0);
    private File logDir = new File(System.getProperty("java.io.tmpdir"), topicPartition.toString());
    private KafkaScheduler scheduler = new KafkaScheduler(1, "scheduler", true);
    private BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
    private LogDirFailureChannel logDirFailureChannel = Mockito.mock(LogDirFailureChannel.class);
    private FileTierPartitionStateFactory factory = new FileTierPartitionStateFactory();
    private TierMetadataManager tierMetadataManager = new TierMetadataManager(factory,
            Optional.empty(), null, false);
    private long nextOffset = 0;
    private LogManager logManager;
    private Partition partition;

    @Setup(Level.Trial)
    public void setUp() {
        boolean observerFeature = Boolean.parseBoolean(observerFeatureStr);
        scheduler.startup();
        LogConfig logConfig = createLogConfig(observerFeature);
        List<File> logDirs = Collections.singletonList(logDir);
        logManager = new LogManager(JavaConverters.asScalaIteratorConverter(logDirs.iterator()).asScala().toSeq(),
                JavaConverters.asScalaIteratorConverter(new ArrayList<File>().iterator()).asScala().toSeq(),
                new scala.collection.mutable.HashMap<>(),
                logConfig,
                new CleanerConfig(0, 0, 0, 0, 0, 0.0, 0, false, "MD5"),
                1,
                1000L,
                10000L,
                10000L,
                1000L,
                60000,
                scheduler,
                new BrokerState(),
                brokerTopicStats,
                logDirFailureChannel,
                tierMetadataManager,
                Time.SYSTEM);
        OffsetCheckpoints offsetCheckpoints = Mockito.mock(OffsetCheckpoints.class);
        Mockito.when(offsetCheckpoints.fetch(logDir.getAbsolutePath(), topicPartition)).thenReturn(Option.apply(0L));
        DelayedOperations delayedOperations = new DelayedOperationsMock();

        // one leader, plus two followers
        List<Integer> replicas = new ArrayList<>();
        replicas.add(0);
        replicas.add(1);
        replicas.add(2);
        LeaderAndIsrPartitionState partitionState = new LeaderAndIsrPartitionState()
            .setTopicName(topicPartition.topic())
            .setPartitionIndex(topicPartition.partition())
            .setControllerEpoch(0)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(replicas)
            .setZkVersion(1)
            .setReplicas(replicas)
            .setIsNew(true);

        List<UpdateMetadataBroker> brokers = new ArrayList<>();
        brokers.add(new UpdateMetadataBroker()
            .setId(0)
            .setEndpoints(Collections.emptyList())
            .setRack("rack-a"));
        brokers.add(new UpdateMetadataBroker()
            .setId(1)
            .setEndpoints(Collections.emptyList())
            .setRack("rack-a"));
        brokers.add(new UpdateMetadataBroker()
            .setId(2)
            .setEndpoints(Collections.emptyList())
            .setRack("rack-b"));

        MetadataCache metadataCache = new MetadataCache(0);
        List<UpdateMetadataPartitionState> partitionStates = Collections.singletonList(
            new UpdateMetadataPartitionState()
                .setTopicName(topicPartition.topic())
                .setPartitionIndex(topicPartition.partition())
                .setControllerEpoch(0)
                .setLeader(0)
                .setLeaderEpoch(0)
                .setIsr(replicas)
                .setZkVersion(1)
                .setReplicas(replicas)
                .setOfflineReplicas(Collections.emptyList()));
        UpdateMetadataRequest request = new UpdateMetadataRequest.Builder(
            ApiKeys.UPDATE_METADATA.latestVersion(), 0, 0, 0,
            partitionStates, brokers).build();
        metadataCache.updateMetadata(0, request);
        PartitionStateStore partitionStateStore = Mockito.mock(PartitionStateStore.class);
        Mockito.when(partitionStateStore.fetchTopicConfig()).thenReturn(new Properties());
        partition = new Partition(topicPartition, 100,
                ApiVersion$.MODULE$.latestVersion(), 0, observerFeature,
                Time.SYSTEM, partitionStateStore, delayedOperations,
                metadataCache, logManager);
        partition.makeLeader(0, partitionState, 0, offsetCheckpoints);
    }

    // avoid mocked DelayedOperations to avoid mocked class affecting benchmark results
    private class DelayedOperationsMock extends DelayedOperations {
        DelayedOperationsMock() {
            super(topicPartition, null, null, null);
        }

        @Override
        public int numDelayedDelete() {
            return 0;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        logManager.shutdown();
        scheduler.shutdown();
    }

    private LogConfig createLogConfig(Boolean observerFeature) {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentMsProp(), Defaults.SegmentMs());
        logProps.put(LogConfig.SegmentBytesProp(), Defaults.SegmentSize());
        logProps.put(LogConfig.RetentionMsProp(), Defaults.RetentionMs());
        logProps.put(LogConfig.RetentionBytesProp(), Defaults.RetentionSize());
        logProps.put(LogConfig.SegmentJitterMsProp(), Defaults.SegmentJitterMs());
        logProps.put(LogConfig.CleanupPolicyProp(), Defaults.CleanupPolicy());
        logProps.put(LogConfig.MaxMessageBytesProp(), Defaults.MaxMessageSize());
        logProps.put(LogConfig.IndexIntervalBytesProp(), Defaults.IndexInterval());
        logProps.put(LogConfig.SegmentIndexBytesProp(), Defaults.MaxIndexSize());
        logProps.put(LogConfig.MessageFormatVersionProp(), Defaults.MessageFormatVersion());
        logProps.put(LogConfig.FileDeleteDelayMsProp(), Defaults.FileDeleteDelayMs());
        if (observerFeature)
            logProps.put(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, "{\"version\":1,"
                + "\"replicas\":[{\"count\":2,\"constraints\":{\"rack\":\"rack-a\"}}],\"observers\":[{\"count\":1,\"constraints\":{\"rack\":\"rack-b\"}}]}");
        return LogConfig.apply(logProps, new scala.collection.immutable.HashSet<>());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void updateFollowerFetchStateBench() {
        // measure the impact of two follower fetches on the leader
        partition.updateFollowerFetchState(1, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, nextOffset);
        partition.updateFollowerFetchState(2, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, nextOffset);
        nextOffset++;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void updateFollowerFetchStateBenchNoChange() {
        // measure the impact of two follower fetches on the leader when the follower didn't
        // end up fetching anything
        partition.updateFollowerFetchState(1, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, 100);
        partition.updateFollowerFetchState(2, new LogOffsetMetadata(nextOffset, nextOffset, 0),
                0, 1, 100);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int caughtUpReplicaCount() {
        return partition.caughtUpReplicaCount();
    }
}
