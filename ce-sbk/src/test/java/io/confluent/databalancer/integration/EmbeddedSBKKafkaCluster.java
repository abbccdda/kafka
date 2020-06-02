/*
 Copyright 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.MockSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampleStore;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore.BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore.PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CONFLUENT_BALANCER_PREFIX;
import static org.junit.Assert.fail;


/**
 * An extension of #{@link EmbeddedKafkaCluster}, this class can be used to create a SBK-enabled Kafka cluster in integration tests.
 *
 * There is some nuance in instantiating the sample store, hence the need for this extension.
 * Typically in tests, we configure Kafka with port 0 which is a placeholder for a free port that's evaluated at runtime.
 * Because we need to configure the sample store with the specific host and port as the bootstrap address, the port of 0 doesn't work.
 * To circumvent this, we first find a free port as part of the test and configure the first broker of the cluster to use that port.
 * All SBK components are configured with the first broker as a bootstrap server.
 */
public class EmbeddedSBKKafkaCluster extends EmbeddedKafkaCluster {
  private int broker0Port;
  private Logger logger;

  /**
   * Start all the brokers, ensuring they have the right
   * bootstrap port configured for the sample store
   */
  @Override
  public void startBrokers(int numBrokers, Properties overrideProps) {
    logger = LoggerFactory.getLogger(this.getClass());
    try {
      broker0Port = findUnusedPort();
    } catch (IOException e) {
      fail(String.format("Could not reserve port due to %s", e));
    }

    Properties properties = buildProperties(overrideProps, broker0Port);

    Properties broker0Props = new Properties(properties);
    broker0Props.putAll(properties);
    broker0Props.setProperty(KafkaConfig.PortProp(), Integer.toString(broker0Port));
    startBroker(0, broker0Props);
    logger.info("Successfully started broker 0");

    properties.setProperty(KafkaConfig.BrokerIdProp(), "1");
    super.startBrokers(numBrokers - 1, properties);
    logger.info("Successfully started the rest of the {} brokers", numBrokers);
  }

  /**
   * Find an unused port on this system, to use for our Kafka
   * SBK needs a bootstrap server, but the standard test config
   * uses a random port (assigned by the system) and the KafkaConfig holds the value "0",
   * which isn't a good bootstrap port. Instead, find an unused port on this machine
   * and use that as our bootstrap value.
   */
  private int findUnusedPort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private Properties buildProperties(Properties overrideProps, int bootstrapPort) {
    Properties properties = new Properties();

    injectSbkProperties(properties, bootstrapPort);
    properties.putAll(overrideProps);
    return properties;
  }

  private void injectSbkProperties(Properties props, int bootstrapPort) {
    props.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, "true");
    props.put(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_CONFIG, "5000000");
    props.put(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_CONFIG, "5000000");
    props.put(confluentBalancerConfig(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG),
        String.format("localhost:%d", bootstrapPort));
    props.put(confluentBalancerConfig(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG),
        "500");
    // use lower partition count for faster rebalances
    props.put(confluentBalancerConfig(PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG), "1");
    props.put(confluentBalancerConfig(BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG), "1");
    // Even though we don't use the metrics reporter, its topic replicas config is used
    props.put(ConfluentMetricsReporterConfig.TOPIC_REPLICAS_CONFIG, "2");
    // larger concurrency limits for faster rebalances
    props.put(confluentBalancerConfig(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG), "50");
    props.put(confluentBalancerConfig(KafkaCruiseControlConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG), "50");

    injectSbkMetricCollectionProperties(props);
  }

  /**
   * Inject the necessary properties to configure SBK's metric collection
   */
  private void injectSbkMetricCollectionProperties(Properties props) {
    // require just 1 window to not collect metrics for too long
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG), Integer.toString(1));
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG), MockSampler.class.getName());
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.SAMPLE_STORE_CLASS_CONFIG), NoopSampleStore.class.getName());
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG), "500");
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG), "501"); // must be larger than metadata refresh period
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG), "1");
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG), "700");
    props.setProperty(confluentBalancerConfig(KafkaCruiseControlConfig.BROKER_METRICS_WINDOW_MS_CONFIG), "700");
  }

  private String confluentBalancerConfig(String cruiseControlConfig) {
    return CONFLUENT_BALANCER_PREFIX + cruiseControlConfig;
  }
}
