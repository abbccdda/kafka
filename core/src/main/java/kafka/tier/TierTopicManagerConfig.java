/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.server.KafkaConfig;
import java.time.Duration;
import java.util.List;

public class TierTopicManagerConfig {
    public final String bootstrapServers;
    public final String tierNamespace;
    public final short numPartitions;
    public final short replicationFactor;
    public final int brokerId;
    public final String clusterId;
    public final Duration pollDuration;
    public final Integer commitIntervalMs;
    public final Integer requestTimeoutMs;
    public final List<String> logDirs;

    public TierTopicManagerConfig(String bootstrapServers,
                                  String tierNamespace,
                                  short numPartitions,
                                  short replicationFactor,
                                  int brokerId,
                                  String clusterId,
                                  Long pollDurationMs,
                                  Integer requestTimeoutMs,
                                  Integer commitIntervalMs,
                                  List<String> logDirs) {
        this.bootstrapServers = bootstrapServers;
        this.tierNamespace = tierNamespace;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.brokerId = brokerId;
        this.clusterId = clusterId;
        this.pollDuration = Duration.ofMillis(pollDurationMs);
        this.requestTimeoutMs = requestTimeoutMs;
        this.commitIntervalMs = commitIntervalMs;
        this.logDirs = logDirs;
    }

    public TierTopicManagerConfig(KafkaConfig config,
                                  String clusterId) {
        this(config.tierMetadataBootstrapServers(),
                config.tierMetadataNamespace(),
                config.tierMetadataNumPartitions(),
                config.tierMetadataReplicationFactor(),
                config.brokerId(),
                clusterId,
                config.tierMetadataMaxPollMs(),
                config.tierMetadataRequestTimeoutMs(),
                config.tierPartitionStateCommitIntervalMs(),
                scala.collection.JavaConversions.seqAsJavaList(config.logDirs()));
    }
}
