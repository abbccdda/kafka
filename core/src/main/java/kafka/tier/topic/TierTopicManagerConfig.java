/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.server.KafkaConfig;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

public class TierTopicManagerConfig {
    public final Supplier<String> bootstrapServersSupplier;
    public final String tierNamespace;
    public final short configuredNumPartitions;
    public final short configuredReplicationFactor;
    public final int brokerId;
    public final String clusterId;
    public final Duration pollDuration;
    public final Integer commitIntervalMs;
    public final Integer requestTimeoutMs;
    public final List<String> logDirs;

    public TierTopicManagerConfig(Supplier<String> bootstrapServersSupplier,
                                  String tierNamespace,
                                  short configuredNumPartitions,
                                  short configuredReplicationFactor,
                                  int brokerId,
                                  String clusterId,
                                  Long pollDurationMs,
                                  Integer requestTimeoutMs,
                                  Integer commitIntervalMs,
                                  List<String> logDirs) {
        this.bootstrapServersSupplier = bootstrapServersSupplier;
        this.tierNamespace = tierNamespace;
        this.configuredNumPartitions = configuredNumPartitions;
        this.configuredReplicationFactor = configuredReplicationFactor;
        this.brokerId = brokerId;
        this.clusterId = clusterId;
        this.pollDuration = Duration.ofMillis(pollDurationMs);
        this.requestTimeoutMs = requestTimeoutMs;
        this.commitIntervalMs = commitIntervalMs;
        this.logDirs = logDirs;
    }

    public TierTopicManagerConfig(KafkaConfig config, Supplier<String> bootstrapServersSupplier, String clusterId) {
        this(bootstrapServersSupplier,
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
