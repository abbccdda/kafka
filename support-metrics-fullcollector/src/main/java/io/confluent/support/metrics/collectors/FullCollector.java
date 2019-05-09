/**
 * Copyright 2015 Confluent Inc.
 *
 * All rights reserved.
 */

package io.confluent.support.metrics.collectors;

import kafka.server.ReplicaManager;
import kafka.zk.AdminZkClient;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.confluent.support.metrics.BrokerMetricsRecord;
import io.confluent.support.metrics.BrokerStatisticsRecord;
import io.confluent.support.metrics.ClusterMetricsRecord;
import io.confluent.support.metrics.RuntimePropertiesRecord;
import io.confluent.support.metrics.SupportKafkaMetricsEnhanced;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.Uuid;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.kafka.KafkaUtilities;
import io.confluent.support.metrics.common.time.TimeUtils;

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;

import static scala.collection.JavaConversions.seqAsJavaList;

public class FullCollector extends Collector {

  private static final Logger log = LoggerFactory.getLogger(FullCollector.class);
  private static final scala.collection.Set<String> EMPTY_TOPICS =
      scala.collection.Set$.MODULE$.empty();
  private static final boolean NO_ERROR_ON_UNAVAILABLE_ENDPOINTS = true;

  /* number of buckets to keep track of replication histogram */
  public static final int NUM_BUCKETS_REPLICA_HISTOGRAM = 6;
  private static final int NUM_BUCKETS_UNCLEAN_LEADER_ELECTION_HISTOGRAM = 2;
  private static final String ZK_CMD_STAT = "stat";
  private final KafkaServer server;
  private final Properties serverConfiguration;
  private final Runtime serverRuntime;
  private final TimeUtils time;
  private final Uuid uuid;
  private final BrokerConfigurationFilter brokerConfigurationFilter;
  private final SystemPropertiesFilter systemPropertiesFilter;

  public FullCollector(
      KafkaServer server,
      Properties serverConfiguration,
      Runtime serverRuntime,
      TimeUtils time
  ) {
    this(server, serverConfiguration, serverRuntime, time, new Uuid());
  }

  public FullCollector(
      KafkaServer server,
      Properties serverConfiguration,
      Runtime serverRuntime,
      TimeUtils time,
      Uuid uuid
  ) {
    super();
    this.server = server;
    this.serverConfiguration = serverConfiguration;
    this.serverRuntime = serverRuntime;
    this.time = time;
    this.uuid = uuid;
    this.brokerConfigurationFilter = new BrokerConfigurationFilter();
    this.systemPropertiesFilter = new SystemPropertiesFilter();
  }

  /**
   * @return A new metrics record, or null in case of any errors.
   */
  @Override
  public GenericContainer collectMetrics() {
    SupportKafkaMetricsEnhanced metricsRecord = new SupportKafkaMetricsEnhanced();
    RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
    BrokerMetricsRecord brokerMetricsRecord = new BrokerMetricsRecord();
    ClusterMetricsRecord clusterMetrics = new ClusterMetricsRecord();

    // populate the basic metrics
    metricsRecord.setTimestamp(time.nowInUnixTime());
    metricsRecord.setClusterId(server.clusterId());
    metricsRecord.setBrokerId(
        Integer.parseInt(
            serverConfiguration.getProperty(
                KafkaConfig$.MODULE$.BrokerIdProp()
            )
        )
    );
    metricsRecord.setJvmStartTimeMs(rb.getStartTime());
    metricsRecord.setJvmUptimeMs(rb.getUptime());
    metricsRecord.setKafkaVersion(AppInfoParser.getVersion());
    metricsRecord.setConfluentPlatformVersion(Version.getVersion());
    metricsRecord.setCollectorState(this.getRuntimeState().stateId());
    metricsRecord.setBrokerProcessUUID(uuid.toString());

    // populate the rest of the metrics
    metricsRecord.setBrokerMetrics(brokerMetricsRecord);
    metricsRecord.setClusterMetrics(clusterMetrics);

    populateBrokerConfiguration(metricsRecord);
    populateJavaSystemProperties(metricsRecord);
    populateJvmRuntimeProperties(metricsRecord);
    populateBrokerStatistics(metricsRecord);
    populateClusterStatistics(metricsRecord);
    populateZookeeper(metricsRecord);
    return metricsRecord;
  }

  private void populateZookeeper(SupportKafkaMetricsEnhanced supportKafkaMetrics) {
    ClusterMetricsRecord clusterMetricsRecord = supportKafkaMetrics.getClusterMetrics();
    final int timeoutMs = 10000;
    String zkConnect = serverConfiguration.getProperty(KafkaConfig$.MODULE$.ZkConnectProp());
    if (zkConnect == null) {
      return;
    }

    ConnectStringParser connectStringParser = new ConnectStringParser(zkConnect);
    ArrayList<InetSocketAddress> zkServers = connectStringParser.getServerAddresses();
    HashMap<String, String>[] zkStats = new HashMap[zkServers.size()];

    // Send each server a 4-letter command
    for (int z = 0; z < zkServers.size(); z++) {
      String hostname = zkServers.get(z).getHostName();
      int port = zkServers.get(z).getPort();
      try (Socket zkSocket = new Socket(hostname, port);
          OutputStreamWriter out = new OutputStreamWriter(zkSocket.getOutputStream(), "UTF-8");
          BufferedReader in = new BufferedReader(
              new InputStreamReader(zkSocket.getInputStream(), StandardCharsets.UTF_8)
          )
      ) {

        zkSocket.setSoTimeout(timeoutMs);
        out.write(ZK_CMD_STAT);
        out.flush();

        zkStats[z] = new HashMap<>();
        String line;
        while ((line = in.readLine()) != null) {
          String[] tokens = line.split(":");
          if (!tokens[0].startsWith("Zookeeper version")) {
            continue;
          }
          if (tokens.length > 1) {
            zkStats[z].put(tokens[0], tokens[1]);
          } else {
            zkStats[z].put(tokens[0], "unknown");
          }
        }
      } catch (UnknownHostException e) {
        log.error("Unknown host {}: {} ", hostname, port, e.getMessage());
      } catch (IOException e) {
        log.error("Failed to communicate with {} at port {}: {} ", hostname, port, e.getMessage());
      }
    }
    clusterMetricsRecord.setZookeeperStats(Arrays.asList((Map<String, String>[]) zkStats));
  }

  /**
   * Populate the broker configuration properties
   */
  private void populateBrokerConfiguration(SupportKafkaMetricsEnhanced supportKafkaMetrics) {
    BrokerMetricsRecord brokerMetricsRecord = supportKafkaMetrics.getBrokerMetrics();
    Properties filteredBrokerConfiguration = brokerConfigurationFilter.apply(serverConfiguration);
    brokerMetricsRecord.setBrokerConfiguration(propertiesToMap(filteredBrokerConfiguration));
  }

  private Map<String, String> propertiesToMap(Properties properties) {
    Map<String, String> m = new HashMap<>();
    for (Object key : properties.keySet()) {
      m.put(key.toString(), properties.get(key).toString());
    }
    return m;
  }

  private void populateJvmRuntimeProperties(SupportKafkaMetricsEnhanced supportKafkaMetrics) {
    BrokerMetricsRecord brokerMetricsRecord = supportKafkaMetrics.getBrokerMetrics();
    RuntimePropertiesRecord runtimePropertiesRecord = new RuntimePropertiesRecord();
    brokerMetricsRecord.setJvmRuntimeEnvironment(runtimePropertiesRecord);

    runtimePropertiesRecord.setAvailableProcessors(serverRuntime.availableProcessors());
    runtimePropertiesRecord.setFreeMemoryBytes(serverRuntime.freeMemory());
    runtimePropertiesRecord.setMaxMemoryBytes(serverRuntime.maxMemory());
    runtimePropertiesRecord.setTotalMemoryBytes(serverRuntime.totalMemory());
  }

  private void populateJavaSystemProperties(SupportKafkaMetricsEnhanced supportKafkaMetrics) {
    BrokerMetricsRecord brokerMetricsRecord = supportKafkaMetrics.getBrokerMetrics();
    Properties filteredSystemProperties = systemPropertiesFilter.apply(System.getProperties());
    brokerMetricsRecord.setJavaSystemProperties(propertiesToMap(filteredSystemProperties));
  }

  private void populateBrokerStatistics(SupportKafkaMetricsEnhanced supportKafkaMetrics) {
    BrokerStatisticsRecord brokerStatisticsRecord = new BrokerStatisticsRecord();
    brokerStatisticsRecord.setWrittenBytes(
        server.brokerTopicStats().allTopicsStats().bytesInRate().count()
    );
    brokerStatisticsRecord.setReadBytes(
        server.brokerTopicStats().allTopicsStats().bytesOutRate().count()
    );
    brokerStatisticsRecord.setBytesInRate(
        server.brokerTopicStats().allTopicsStats().bytesInRate().meanRate()
    );
    brokerStatisticsRecord.setBytesOutRate(
        server.brokerTopicStats().allTopicsStats().bytesOutRate().meanRate()
    );
    if (server.replicaManager() != null
        && server.replicaManager().partitionCount() != null) {
      brokerStatisticsRecord.setNumPartitions((Integer) server
          .replicaManager()
          .partitionCount()
          .value());
    } else {
      // We still want to collect the remaining stats during testing in case the server is e.g.
      // null.
      brokerStatisticsRecord.setNumPartitions(0);
    }

    BrokerMetricsRecord brokerMetricsRecord = supportKafkaMetrics.getBrokerMetrics();
    brokerMetricsRecord.setBrokerStatistics(brokerStatisticsRecord);
  }

  private void populateClusterStatistics(SupportKafkaMetricsEnhanced supportKafkaMetrics) {
    // populate metrics related to replication and consistency
    ClusterMetricsRecord clusterMetricsRecord = supportKafkaMetrics.getClusterMetrics();
    Integer[] replicaHistogram = new Integer[NUM_BUCKETS_REPLICA_HISTOGRAM];
    Integer[] minIsrHistogram = new Integer[NUM_BUCKETS_REPLICA_HISTOGRAM];
    Integer[] uncleanLeaderElectionHistogram =
        new Integer[NUM_BUCKETS_UNCLEAN_LEADER_ELECTION_HISTOGRAM];

    // Will track the number of topics by querying the broker's metadata cache.  It is expected that
    // the cache will be behind zookeeper's view, which we query further down.  So we collect both
    // metrics.
    long numTopicsBasedOnBrokerMetadataCache = 0;

    long totalNumberOfPartitionsBasedOnBrokerMetadataCache = 0;
    for (int i = 0; i < NUM_BUCKETS_REPLICA_HISTOGRAM; i++) {
      replicaHistogram[i] = 0;
      minIsrHistogram[i] = 0;
    }
    for (int i = 0; i < NUM_BUCKETS_UNCLEAN_LEADER_ELECTION_HISTOGRAM; i++) {
      uncleanLeaderElectionHistogram[i] = 0;
    }

    // iterate over all the cached metadata
    if (server != null) {
      // Query zookeeper to get zookeeper's view of number of topics in cluster.
      long numTopicsBasedOnZookeeperData = new KafkaUtilities().getNumTopics(server.zkClient());
      clusterMetricsRecord.setNumberTopicsZk(numTopicsBasedOnZookeeperData);

      Set<String> topics = new HashSet<>();
      AdminZkClient adminZkClient = new AdminZkClient(server.zkClient());

      for (ListenerName listener : listenerNames(supportKafkaMetrics.getBrokerId())) {
        final List<MetadataResponse.TopicMetadata> topicMetadataList;

        try {
          topicMetadataList = seqAsJavaList(
              server.metadataCache().getTopicMetadata(
                  EMPTY_TOPICS, listener, NO_ERROR_ON_UNAVAILABLE_ENDPOINTS,
                  NO_ERROR_ON_UNAVAILABLE_ENDPOINTS
              )
          );
        } catch (Exception e) {
          // getTopicMetadata throws exceptions when no brokers are found for a given security
          // protocol
          // this code simply collects stats about any available brokers and has to continue
          log.debug("Could not retrieve metadata for topic with listener {}.", listener, e);
          continue;
        }

        for (MetadataResponse.TopicMetadata topicMetadata : topicMetadataList) {
          String topic = topicMetadata.topic();
          if (!topics.add(topic)) {
            continue;
          }

          for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata
              .partitionMetadata()) {
            //
            // Get min isr histogram
            // Get uncleanLeaderElection histogram too in the same block since this block
            // will succeed if we are leader for a topic
            TopicPartition tp = new TopicPartition(topic, partitionMetadata.partition());
            ReplicaManager replicaManager = server.replicaManager();
            try {
              // Assert leadership for this partition
              replicaManager.getPartitionOrException(tp, true);

              int minIsr = replicaManager.getLogConfig(tp).get().minInSyncReplicas();

              if (minIsr >= 0) {
                if (minIsr >= NUM_BUCKETS_REPLICA_HISTOGRAM) {
                  minIsrHistogram[NUM_BUCKETS_REPLICA_HISTOGRAM - 1]++;
                } else {
                  minIsrHistogram[minIsr]++;
                }
              }

              if (LogConfig.fromProps(
                  server.config().originals(),
                  adminZkClient.fetchEntityConfig(
                      ConfigType.Topic(),
                      topicMetadata.topic()
                  )
              ).uncleanLeaderElectionEnable()) {
                uncleanLeaderElectionHistogram[1]++;
              } else {
                uncleanLeaderElectionHistogram[0]++;
              }

            } catch (Exception e) {
              // Throws exceptions to when this broker is not leader for topic.
              // This code has to continue to next.
              log.debug(
                  "Could not check if leader replica is local for topic partition {}.",
                  tp,
                  e
              );
            }

            List<Node> replicas = partitionMetadata.replicas();
            int numReplicas = replicas.size();
            if (numReplicas >= 0) {
              if (numReplicas >= NUM_BUCKETS_REPLICA_HISTOGRAM) {
                replicaHistogram[NUM_BUCKETS_REPLICA_HISTOGRAM - 1]++;
              } else {
                replicaHistogram[numReplicas]++;
              }
            }
            totalNumberOfPartitionsBasedOnBrokerMetadataCache++;
          }
          numTopicsBasedOnBrokerMetadataCache++;
        }
      }
    }
    clusterMetricsRecord.setReplicationHistogram(Arrays.asList(replicaHistogram));
    clusterMetricsRecord.setNumberPartitions(totalNumberOfPartitionsBasedOnBrokerMetadataCache);
    clusterMetricsRecord.setNumberTopics(numTopicsBasedOnBrokerMetadataCache);
    clusterMetricsRecord.setMinIsrHistogram(Arrays.asList(minIsrHistogram));
    clusterMetricsRecord.setUncleanLeaderElectionHistogram(
        Arrays.asList(uncleanLeaderElectionHistogram)
    );
  }

  private Set<ListenerName> listenerNames(int brokerId) {
    Broker broker = server.zkClient().getBroker(brokerId).get();
    Set<ListenerName> listeners = new HashSet<>();
    for (EndPoint endpoint : seqAsJavaList(broker.endPoints())) {
      listeners.add(endpoint.listenerName());
    }
    return listeners;
  }

}
