/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import kafka.common.TopicPlacement;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MetadataClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataClient.class);
  private final AtomicInteger _metadataGeneration;
  private final Time _time;
  private final long _metadataTTLMs;
  private final Admin _adminClient;
  private final int _refreshMetadataTimeoutMs;

  private long _lastSuccessfulUpdateMs;
  private ClusterAndPlacements _clusterAndPlacements;

  long _version;

  public MetadataClient(KafkaCruiseControlConfig config,
                        long metadataTTLMs,
                        Time time) {
    this(config, metadataTTLMs, time, KafkaCruiseControlUtils.createAdmin(config.values()));
  }

  // for testing
  MetadataClient(KafkaCruiseControlConfig config,
                 long metadataTTLMs,
                 Time time,
                 Admin adminClient) {
    _metadataGeneration = new AtomicInteger(0);
    _refreshMetadataTimeoutMs = config.getInt(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG);
    _adminClient = adminClient;
    _time = time;
    _metadataTTLMs = metadataTTLMs;
    _version = 0;
    _lastSuccessfulUpdateMs = 0;
    _clusterAndPlacements = new ClusterAndPlacements(Cluster.empty(), Collections.emptyMap());
  }

  /**
   * Refresh the metadata.
   */
  public ClusterAndGeneration refreshMetadata() {
    return refreshMetadata(_refreshMetadataTimeoutMs);
  }

  /**
   * Refresh the metadata. Synchronized to prevent concurrent updates to the metadata cache
   */
  public synchronized ClusterAndGeneration refreshMetadata(int timeoutMs) {
    // Do not update metadata if the metadata has just been refreshed.
    if (_time.milliseconds() >= _lastSuccessfulUpdateMs + _metadataTTLMs) {
      try {
        // Cruise Control always fetches metadata for all the topics.
        ClusterAndPlacements refreshedCluster = doRefreshMetadata(timeoutMs);
        _lastSuccessfulUpdateMs = _time.milliseconds();
        _version += 1;
        LOG.debug("Updated metadata {}", _clusterAndPlacements.cluster());
        if (MonitorUtils.metadataChanged(_clusterAndPlacements, refreshedCluster)) {
          _metadataGeneration.incrementAndGet();
          _clusterAndPlacements = new ClusterAndPlacements(refreshedCluster.cluster(), refreshedCluster.topicPlacements());
        }
      } catch (ExecutionException | TimeoutException | InterruptedException e) {
        LOG.warn("Exception while updating metadata", e);
        LOG.warn("Failed to update metadata in {}ms. Using old metadata with version {} and last successful update {}.",
            timeoutMs, _version, _lastSuccessfulUpdateMs);
      }
    }
    return new ClusterAndGeneration(_clusterAndPlacements, _metadataGeneration.get());
  }

  private ClusterAndPlacements doRefreshMetadata(int timeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
    // We use both AdminClient timeout options and `future.get(timeout)`
    // The former ensures the timeouts are enforced in the brokers
    int remainingMs = timeoutMs;
    long startMs = _time.milliseconds();

    Set<String> topicNames = _adminClient.listTopics(new ListTopicsOptions().timeoutMs(remainingMs).listInternal(true))
        .names().get(remainingMs, TimeUnit.MILLISECONDS);
    long endMs = _time.milliseconds();
    long elapsedTimeMs = endMs - startMs;
    remainingMs -= elapsedTimeMs;

    DescribeClusterResult result = _adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(remainingMs));
    Collection<Node> nodes = result.nodes().get(remainingMs, TimeUnit.MILLISECONDS);
    String clusterId = result.clusterId().get(remainingMs, TimeUnit.MILLISECONDS);
    Node controller = result.controller().get(remainingMs, TimeUnit.MILLISECONDS);
    elapsedTimeMs = _time.milliseconds() - endMs;
    remainingMs -= elapsedTimeMs;

    DescribeTopicsResult describeTopicsResult = _adminClient.describeTopics(topicNames, new DescribeTopicsOptions().timeoutMs(remainingMs));
    DescribeConfigsResult describeConfigsResult = _adminClient.describeConfigs(
            topicNames.stream().map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic)).collect(Collectors.toList()),
            new DescribeConfigsOptions().timeoutMs(remainingMs));

    Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get(remainingMs, TimeUnit.MILLISECONDS);
    elapsedTimeMs = _time.milliseconds() - endMs;
    remainingMs -= elapsedTimeMs;
    Map<ConfigResource, Config> topicConfigs = describeConfigsResult.all().get(remainingMs, TimeUnit.MILLISECONDS);

    return new ClusterAndPlacements(cluster(clusterId, nodes, controller, topicDescriptions),
           toTopicPlacements(topicConfigs));
  }

  private Cluster cluster(String clusterId, Collection<Node> nodes, Node controller,
                          Map<String, TopicDescription> describeTopicResult) {
    List<PartitionInfo> partitionInfos = new LinkedList<>();
    Set<String> internalTopics = new HashSet<>();

    for (Map.Entry<String, TopicDescription> topicDescription : describeTopicResult.entrySet()) {
      if (topicDescription.getValue().isInternal()) {
        internalTopics.add(topicDescription.getKey());
      }

      // Unlike the NetworkClient's metadata, AdminClient does not return offline replicas from the describeConfigs
      // call. To identify offline replicas, we use the same behavior as the kafka-topics command and mark any replica
      // that is not on a live broker as offline. This does not handle the case when JBOD is enabled and one disk on an
      // otherwise healthy broker is offline, but since we currently don't support JBOD with SBK, this is acceptable.
      for (TopicPartitionInfo topicPartitionInfo : topicDescription.getValue().partitions()) {
        PartitionInfo partitionInfo = PartitionInfo.of(topicDescription.getKey(),
            topicPartitionInfo.partition(),
            topicPartitionInfo.leader(),
            topicPartitionInfo.replicas().toArray(new Node[0]),
            topicPartitionInfo.observers().toArray(new Node[0]),
            topicPartitionInfo.isr().toArray(new Node[0]),
            topicPartitionInfo.replicas()
                              .stream()
                              .filter(r -> !nodes.contains(r))
                              .toArray(Node[]::new)
            );
        partitionInfos.add(partitionInfo);
      }
    }
    return new Cluster(clusterId, nodes, partitionInfos, Collections.emptySet(),
        internalTopics, controller);
  }

  private static Map<String, TopicPlacement> toTopicPlacements(Map<ConfigResource, Config> topicConfigs) {
    Map<String, TopicPlacement> topicConfig = topicConfigs.entrySet().stream().flatMap(entry -> {
      ConfigEntry topicPlacementConfig = entry.getValue().get(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG);
      if (topicPlacementConfig != null) {
        try {
          Optional<TopicPlacement> topicPlacement = TopicPlacement.parse(topicPlacementConfig.value());
          if (topicPlacement.isPresent()) {
            return Stream.of(new AbstractMap.SimpleEntry<>(entry.getKey().name(), topicPlacement.get()));
          }
        } catch (IllegalArgumentException e) {
          LOG.warn("Error parsing topic placement config {}. Received exception: {}", topicPlacementConfig.value(), e.getMessage());
          return Stream.empty();
        }
      }
      return Stream.empty();
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return topicConfig;
  }

  /**
   * Close the admin client. Synchronized to avoid calling the admin client during a shutdown
   */
  public synchronized void close() {
    _adminClient.close();
  }

  /**
   * Get the current cluster and generation
   */
  public ClusterAndGeneration clusterAndGeneration() {
    return new ClusterAndGeneration(new ClusterAndPlacements(cluster(), topicPlacements()), _metadataGeneration.get());
  }

  /**
   * Get the current cluster.
   */
  public Cluster cluster() {
    return _clusterAndPlacements.cluster();
  }

  public Map<String, TopicPlacement> topicPlacements() {
    return _clusterAndPlacements.topicPlacements();
  }

  public static class ClusterAndPlacements {
    private final Cluster _cluster;
    private final Map<String, TopicPlacement> _topicPlacements;

    public ClusterAndPlacements(Cluster cluster, Map<String, TopicPlacement> topicPlacements) {
      _cluster = cluster;
      _topicPlacements = topicPlacements;
    }

    public Cluster cluster() {
      return _cluster;
    }

    public Map<String, TopicPlacement> topicPlacements() {
      return _topicPlacements;
    }
  }

  public static class ClusterAndGeneration {
    private final ClusterAndPlacements _clusterAndPlacements;
    private final int _generation;

    public ClusterAndGeneration(ClusterAndPlacements cluster, int generation) {
      _clusterAndPlacements = cluster;
      _generation = generation;
    }

    public Cluster cluster() {
      return _clusterAndPlacements.cluster();
    }

    public Map<String, TopicPlacement> topicPlacements() {
      return _clusterAndPlacements.topicPlacements();
    }

    public int generation() {
      return _generation;
    }
  }
}
