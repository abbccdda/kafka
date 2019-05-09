/**
 * Copyright 2015 Confluent Inc.
 *
 * All rights reserved.
 */
package io.confluent.support.metrics.collectors;

import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.support.metrics.BrokerMetricsRecord;
import io.confluent.support.metrics.BrokerStatisticsRecord;
import io.confluent.support.metrics.ClusterMetricsRecord;
import io.confluent.support.metrics.RuntimePropertiesRecord;
import io.confluent.support.metrics.SupportKafkaMetricsEnhanced;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.Uuid;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.kafka.EmbeddedKafkaCluster;
import io.confluent.support.metrics.common.time.TimeUtils;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test class makes use of {@link EmbeddedKafkaCluster} and thus must be run in its own JVM
 * (you get this behavior by default if you run e.g. "mvn test" from the command line).  This
 * ensures that tests that verify Kafka's metrics will work  properly when run in combination with
 * other tests in this project's test suite.
 *
 * See the top-level README for details, e.g. how to configure IntelliJ IDEA to create a new JVM
 * for each test class.
 */
public class FullCollectorSingleNodeClusterTest {

  @Test
  public void metricsAreCollectedFromSingleNodeClusters() throws IOException {
    // Given
    Uuid uuid = new Uuid();
    TimeUtils time = new TimeUtils();
    long unixTimeAtTestStart = time.nowInUnixTime();
    BrokerConfigurationFilter brokerConfigurationFilter = new BrokerConfigurationFilter();
    SystemPropertiesFilter systemPropertiesFilter = new SystemPropertiesFilter();
    RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
    Runtime serverRuntime = mock(Runtime.class);
    int expNumProcessors = 2;
    when(serverRuntime.availableProcessors()).thenReturn(expNumProcessors);
    long expFreeMemoryBytes = 1024;
    when(serverRuntime.freeMemory()).thenReturn(expFreeMemoryBytes);
    long expMaxMemoryBytes = 1024;
    when(serverRuntime.maxMemory()).thenReturn(expMaxMemoryBytes);
    long expTotalMemoryBytes = 1024;
    when(serverRuntime.totalMemory()).thenReturn(expTotalMemoryBytes);

    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);

    Properties brokerConfiguration = brokerConfigurationFrom(broker, cluster.zookeeperConnectString());
    Collector metricsCollector = new FullCollector(broker, brokerConfiguration, serverRuntime, time, uuid);

    // When
    GenericContainer metricsRecord = metricsCollector.collectMetrics();

    //
    // Then
    //

    // Verify basic properties
    assertThat(metricsRecord).isInstanceOf(SupportKafkaMetricsEnhanced.class);
    assertThat(metricsRecord.getSchema()).isEqualTo(SupportKafkaMetricsEnhanced.getClassSchema());
    SupportKafkaMetricsEnhanced enhancedRecord = (SupportKafkaMetricsEnhanced) metricsRecord;
    assertThat(enhancedRecord.getTimestamp()).isBetween(unixTimeAtTestStart, time.nowInUnixTime());
    assertThat(enhancedRecord.getClusterId()).isEqualTo(broker.clusterId());
    assertThat(enhancedRecord.getBrokerId()).isEqualTo(Integer.valueOf(brokerConfiguration.getProperty(KafkaConfig$.MODULE$.BrokerIdProp())));
    assertThat(enhancedRecord.getJvmStartTimeMs()).isEqualTo(rb.getStartTime());
    assertThat(enhancedRecord.getJvmUptimeMs()).isLessThanOrEqualTo(rb.getUptime());
    assertThat(enhancedRecord.getKafkaVersion()).isEqualTo(AppInfoParser.getVersion());
    assertThat(enhancedRecord.getConfluentPlatformVersion()).isEqualTo(Version.getVersion());
    assertThat(enhancedRecord.getCollectorState()).isEqualTo(metricsCollector.getRuntimeState().stateId());
    assertThat(enhancedRecord.getBrokerProcessUUID()).isEqualTo(uuid.toString());

    // Verify filtered broker configuration
    Properties expFilteredBrokerConfiguration = brokerConfigurationFilter.apply(brokerConfiguration);
    BrokerMetricsRecord brokerMetricsRecord = enhancedRecord.getBrokerMetrics();
    assertThat(brokerMetricsRecord).isNotNull();
    assertThat(brokerMetricsRecord.getSchema()).isEqualTo(BrokerMetricsRecord.getClassSchema());
    Map brokerConfig = brokerMetricsRecord.getBrokerConfiguration();
    assertThat(brokerConfig).isNotNull();
    assertThat(brokerConfig.size()).isEqualTo(expFilteredBrokerConfiguration.size());
    for (Object key : expFilteredBrokerConfiguration.keySet()) {
      assertThat(brokerConfig.containsKey(key)).isTrue();
      assertThat(brokerConfig.get(key)).isEqualTo(expFilteredBrokerConfiguration.get(key));
    }

    // Verify filtered Java system properties
    Properties expFilteredSystemProperties = systemPropertiesFilter.apply(System.getProperties());
    Map javaSystemProperties = brokerMetricsRecord.getJavaSystemProperties();
    assertThat(javaSystemProperties).isNotNull();
    assertThat(javaSystemProperties.size()).isEqualTo(expFilteredSystemProperties.size());
    for (Object key : expFilteredSystemProperties.keySet()) {
      assertThat(javaSystemProperties.containsKey(key)).isTrue();
      assertThat(javaSystemProperties.get(key)).isEqualTo(expFilteredSystemProperties.get(key));
    }

    // Verify JVM runtime properties
    RuntimePropertiesRecord runtimePropertiesRecord = brokerMetricsRecord.getJvmRuntimeEnvironment();
    assertThat(runtimePropertiesRecord).isNotNull();
    assertThat(runtimePropertiesRecord.getSchema()).isEqualTo(RuntimePropertiesRecord.getClassSchema());
    assertThat(runtimePropertiesRecord.getAvailableProcessors()).isEqualTo(serverRuntime.availableProcessors());
    assertThat(runtimePropertiesRecord.getFreeMemoryBytes()).isGreaterThan(0);
    assertThat(runtimePropertiesRecord.getMaxMemoryBytes()).isEqualTo(serverRuntime.maxMemory());
    assertThat(runtimePropertiesRecord.getTotalMemoryBytes()).isEqualTo(serverRuntime.totalMemory());

    // Verify broker statistics
    BrokerStatisticsRecord brokerStatisticsRecord = brokerMetricsRecord.getBrokerStatistics();
    assertThat(brokerStatisticsRecord).isNotNull();
    assertThat(brokerStatisticsRecord.getSchema()).isEqualTo(BrokerStatisticsRecord.getClassSchema());
    assertThat(brokerStatisticsRecord.getBytesInRate()).isEqualTo(0);
    assertThat(brokerStatisticsRecord.getBytesOutRate()).isEqualTo(0);
    assertThat(brokerStatisticsRecord.getWrittenBytes()).isEqualTo(0);
    assertThat(brokerStatisticsRecord.getReadBytes()).isEqualTo(0);
    assertThat(brokerStatisticsRecord.getNumPartitions()).isEqualTo(0);

    // Verify cluster statistics
    ClusterMetricsRecord clusterMetrics = enhancedRecord.getClusterMetrics();
    assertThat(clusterMetrics).isNotNull();
    assertThat(clusterMetrics.getSchema()).isEqualTo(ClusterMetricsRecord.getClassSchema());
    assertThat(clusterMetrics.getNumberTopics()).isEqualTo(0);
    assertThat(clusterMetrics.getNumberTopicsZk()).isEqualTo(0);
    assertThat(clusterMetrics.getNumberPartitions()).isEqualTo(0);

    List<Integer> replicaHistogram = clusterMetrics.getReplicationHistogram();
    assertThat(replicaHistogram).isNotNull();
    for (Integer bucket : replicaHistogram) {
      assertThat(bucket).isEqualTo(0);
    }

    List<Integer> minIsrHistogram = clusterMetrics.getMinIsrHistogram();
    assertThat(minIsrHistogram).isNotNull();
    for (Integer bucket : minIsrHistogram) {
      assertThat(bucket).isEqualTo(0);
    }

    List<Integer> uncleanLeaderElectionHistogram = clusterMetrics.getUncleanLeaderElectionHistogram();
    assertThat(uncleanLeaderElectionHistogram).isNotNull();
    for (Integer bucket : uncleanLeaderElectionHistogram) {
      assertThat(bucket).isEqualTo(0);
    }

    // Verify zookeeper stats
    List<Map<String, String>> zookeeperStats = clusterMetrics.getZookeeperStats();
    assertThat(zookeeperStats).isNotNull();
    assertThat(zookeeperStats.size()).isEqualTo(1);
    assertThat(zookeeperStats.get(0)).isNotNull();
    assertThat(zookeeperStats.get(0).size()).isEqualTo(1);

    // Cleanup
    cluster.stopCluster();
  }

  private Properties brokerConfigurationFrom(KafkaServer broker, String zookeeperConnect) throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(FullCollectorSingleNodeClusterTest.class.getResourceAsStream("/default-server.properties"));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(broker.config().brokerId()));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeperConnect);
    return brokerConfiguration;
  }

}
