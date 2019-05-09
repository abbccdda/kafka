/**
 * Copyright 2015 Confluent Inc.
 *
 * All rights reserved.
 */
package io.confluent.support.metrics.collectors;

import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import kafka.utils.TestUtils;
import org.apache.kafka.common.utils.Utils;
import scala.collection.JavaConversions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.confluent.support.metrics.ClusterMetricsRecord;
import io.confluent.support.metrics.SupportKafkaMetricsEnhanced;
import io.confluent.support.metrics.common.Uuid;
import io.confluent.support.metrics.common.kafka.EmbeddedKafkaCluster;
import io.confluent.support.metrics.common.kafka.KafkaUtilities;
import io.confluent.support.metrics.common.time.TimeUtils;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * This test class makes use of {@link EmbeddedKafkaCluster} and thus must be run in its own JVM
 * (you get this behavior by default if you run e.g. "mvn test" from the command line).  This
 * ensures that tests that verify Kafka's metrics will work  properly when run in combination with
 * other tests in this project's test suite.
 *
 * See the top-level README for details, e.g. how to configure IntelliJ IDEA to create a new JVM
 * for each test class.
 */
public class FullCollectorMultiNodeClusterTest {

  private static final long ONE_YEAR_RETENTION = 365 * 24 * 60 * 60 * 1000L;
  private static final byte[] ANY_BYTES = "any bytes".getBytes();

  @Test
  public void metricsAreCollectedFromMultiNodeClusters() throws IOException, InterruptedException {
    // Given
    Uuid uuid = new Uuid();
    TimeUtils time = new TimeUtils();
    Runtime serverRuntime = Runtime.getRuntime();

    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 10;
    cluster.startCluster(numBrokers);
    KafkaServer firstBroker = cluster.getBroker(0);

    // Create a collector for each broker.
    List<FullCollector> collectors = new ArrayList<>();
    for (int i = 0; i < numBrokers; i++) {
      KafkaServer broker = cluster.getBroker(i);
      Properties brokerConfiguration = brokerConfigurationFrom(broker, cluster.zookeeperConnectString());
      collectors.add(new FullCollector(cluster.getBroker(i), brokerConfiguration, serverRuntime, time, uuid));
    }

    int numTopics = 20;
    TotalPartitionsAndTotalReplicaHistogram totalPartitionsAndTotalReplicaHistogram =
        createTopicsWithMessages(numTopics, numBrokers, cluster, firstBroker.zkClient());

    for (FullCollector collector : collectors) {
      // When
      SupportKafkaMetricsEnhanced enhancedRecord = (SupportKafkaMetricsEnhanced) collector.collectMetrics();

      // Then
      assertThat(enhancedRecord.getClusterId()).isEqualTo(firstBroker.clusterId());
      ClusterMetricsRecord clusterMetrics = enhancedRecord.getClusterMetrics();
      // Metadata might not be updated at all brokers, so each broker has a partial view of the
      // topics and partitions. Zookeeper has the exact number of topics.
      assertThat(clusterMetrics.getNumberTopicsZk()).isEqualTo(numTopics);
      assertThat(clusterMetrics.getNumberTopics()).isLessThanOrEqualTo(numTopics);
      assertThat(clusterMetrics.getNumberPartitions()).isLessThanOrEqualTo(totalPartitionsAndTotalReplicaHistogram.getTotalPartitions());
      List<Integer> replicaHistogram = clusterMetrics.getReplicationHistogram();
      Integer[] totalReplicaHistogram = totalPartitionsAndTotalReplicaHistogram.getTotalReplicaHistogram();
      for (int r = 0; r < FullCollector.NUM_BUCKETS_REPLICA_HISTOGRAM; r++) {
        assertThat(replicaHistogram.get(r)).isLessThanOrEqualTo(totalReplicaHistogram[r]);
      }
    }

    // Cleanup
    cluster.stopCluster();
  }

  private Properties brokerConfigurationFrom(KafkaServer broker, String zookeeperConnect) throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(FullCollectorMultiNodeClusterTest.class.getResourceAsStream("/default-server.properties"));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(broker.config().brokerId()));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeperConnect);
    return brokerConfiguration;
  }

  private TotalPartitionsAndTotalReplicaHistogram createTopicsWithMessages(
      int numTopics, int numBrokers, EmbeddedKafkaCluster cluster, KafkaZkClient zkClient)
      throws InterruptedException {
    KafkaUtilities kUtil = new KafkaUtilities();
    int totalPartitions = 0;
    Integer[] totalReplicaHistogram = new Integer[FullCollector.NUM_BUCKETS_REPLICA_HISTOGRAM];
    for (int i = 0; i < totalReplicaHistogram.length; i++) {
      totalReplicaHistogram[i] = 0;
    }
    Random rand = new Random();
    for (int t = 0; t < numTopics; t++) {
      String topic = "__exampleTopic" + t;
      int partitions = rand.nextInt(10) + 2;
      int replication = rand.nextInt(numBrokers) + 1;
      String topicDesc = "Topic='" + topic + "', partitions=" + partitions
                         + ", replication=" + replication;

      if (!kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, ONE_YEAR_RETENTION)) {
        fail("Failed to create and/or verify topic " + topicDesc);
      }
      waitUntilMetadataIsPropagated(topic, partitions, cluster, numBrokers);

      List<String> bootstrapServers = kUtil.getBootstrapServers(zkClient, numBrokers);
      Producer<byte[], byte[]> producer = createProducer(bootstrapServers);

      // Produce at least one record per topic.  This ensures that the metadata cache of each broker
      // where partitions are stored will be updated to know about the topic and its settings such
      // as replication factor.
      Future response = producer.send(new ProducerRecord<byte[], byte[]>(topic, ANY_BYTES));
      producer.flush();

      try {
        response.get();
      } catch (InterruptedException e) {
        fail("Failed to submit metrics to Kafka topic (canceled request). " + topicDesc);
      } catch (ExecutionException e) {
        fail("Failed to submit metrics to Kafka topic (due to exception=" + e.getMessage()
             + "). " + topicDesc);
      }
      totalPartitions += partitions;
      int replicaHistogramBucket = 0;
      if (replication >= 0) {
        if (replication >= totalReplicaHistogram.length) {
          replicaHistogramBucket = totalReplicaHistogram.length - 1;
        } else {
          replicaHistogramBucket = replication;
        }
      }
      totalReplicaHistogram[replicaHistogramBucket] += partitions;
    }
    return new TotalPartitionsAndTotalReplicaHistogram(totalPartitions, totalReplicaHistogram);
  }

  private static class TotalPartitionsAndTotalReplicaHistogram {

    private final int totalPartitions;
    private final Integer[] totalReplicaHistogram;

    public TotalPartitionsAndTotalReplicaHistogram(int totalPartitions, Integer[] totalReplicaHistogram) {
      this.totalPartitions = totalPartitions;
      this.totalReplicaHistogram = totalReplicaHistogram;
    }

    public int getTotalPartitions() {
      return totalPartitions;
    }

    public Integer[] getTotalReplicaHistogram() {
      return totalReplicaHistogram;
    }

  }

  private Producer<byte[], byte[]> createProducer(List<String> bootstrapServers) {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.join(bootstrapServers, ","));
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    return new KafkaProducer<>(producerProps);
  }

  private void waitUntilMetadataIsPropagated(
      String topic, int partitions, EmbeddedKafkaCluster cluster, int numBrokers) {
    Set<KafkaServer> servers = new HashSet<>();
    for (int i = 0; i < numBrokers; ++i) {
      servers.add(cluster.getBroker(i));
    }
    for (int part = 0; part < partitions; ++part) {
      int leader = TestUtils.waitUntilMetadataIsPropagated(
          JavaConversions.asScalaSet(servers).toSeq(), topic, part, 30000L);
      if (leader < 0) {
        fail("Topic=" + topic + " partition=" + part + " metadata was not propagated.");
      }
    }
  }

}
