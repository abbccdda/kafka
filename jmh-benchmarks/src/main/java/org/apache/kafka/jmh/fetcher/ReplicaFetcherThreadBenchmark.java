package org.apache.kafka.jmh.fetcher;

import kafka.cluster.BrokerEndPoint;
import kafka.log.LogAppendInfo;
import kafka.server.FailedPartitions;
import kafka.server.KafkaConfig;
import kafka.server.OffsetAndEpoch;
import kafka.server.OffsetTruncationState;
import kafka.server.ReplicaFetcherThread;
import kafka.server.ReplicaQuota;
import kafka.tier.TierMetadataManager;
import kafka.tier.state.FileTierPartitionStateFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.Time;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;
import scala.collection.Map;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class ReplicaFetcherThreadBenchmark {
    @Param({"500", "5000", "10000"})
    public static String partitionCount;

    @State(Scope.Thread)
    public static class BenchState {
        ReplicaFetcherBenchThread fetcher;

        public BenchState() {
            Properties props = new Properties();
            props.put("zookeeper.connect", "127.0.0.1:9999");
            FileTierPartitionStateFactory factory = new FileTierPartitionStateFactory();
            TierMetadataManager tierMetadataManager = new TierMetadataManager(factory, Optional.empty(), null, false);
            KafkaConfig config = new KafkaConfig(props);
            fetcher = new ReplicaFetcherBenchThread(config, tierMetadataManager);
            scala.collection.mutable.Map<TopicPartition, OffsetAndEpoch> partitions = new scala.collection.mutable.HashMap<>();
            for (int i = 0; i < Integer.parseInt(partitionCount); i++) {
                partitions.put(new TopicPartition("mytopic", i), new OffsetAndEpoch(0, 0));
            }
            fetcher.addPartitions(partitions);
            // force a pass to move partitions to fetching state
            // we do this in the constructor so that we do not measure this time as part of the
            // steady state work
            fetcher.doWork();
        }
    }

    @Benchmark
    public long testFetcher(BenchState state) {
        state.fetcher.doWork();
        return state.fetcher.fetcherStats().requestRate().count();
    }
}

class ReplicaFetcherBenchThread extends ReplicaFetcherThread {
    public ReplicaFetcherBenchThread(KafkaConfig config, TierMetadataManager tierMetadataManager) {
        super("name",
                3,
                new BrokerEndPoint(3, "host", 3000),
                config,
                new FailedPartitions(),
                null,
                new Metrics(),
                Time.SYSTEM,
                new ReplicaQuota() {
                    @Override
                    public boolean isQuotaExceeded() {
                        return false;
                    }
                    @Override
                    public void record(long value) { }
                    @Override
                    public boolean isThrottled(TopicPartition topicPartition) {
                        return false;
                    }
                },
                tierMetadataManager,
                Option.empty(),
                Option.empty());
    }

    @Override
    public Option<Object> latestEpoch(TopicPartition topicPartition) {
        return Option.apply(0);
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
        return Option.empty();
    }

    @Override
    public long fetchEarliestLocalOffsetFromLeader(TopicPartition topicPartition, int currentLeaderEpoch) {
        return 0;
    }

    @Override
    public long fetchEarliestOffsetFromLeader(TopicPartition topicPartition, int currentLeaderEpoch) {
        return 0;
    }

    @Override
    public long logStartOffset(TopicPartition topicPartition) {
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
    public Seq<Tuple2<TopicPartition, FetchResponse.PartitionData<Records>>> fetchFromLeader(FetchRequest.Builder fetchRequest) {
        return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<TopicPartition, FetchResponse.PartitionData<Records>>>().iterator()).asScala().toSeq();
    }
}
