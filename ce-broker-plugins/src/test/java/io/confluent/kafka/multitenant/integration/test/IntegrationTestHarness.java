// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Time;

public class IntegrationTestHarness {

  private static final int DEFAULT_BROKERS_IN_PHYSICAL_CLUSTER = 1;

  private PhysicalCluster physicalCluster;
  private final List<KafkaProducer<?, ?>> producers = new ArrayList<>();
  private final List<KafkaConsumer<?, ?>> consumers = new ArrayList<>();
  private final List<AdminClient> adminClients = new ArrayList<>();
  private final int brokersInPhysicalCluster;

  public IntegrationTestHarness() {
    this(DEFAULT_BROKERS_IN_PHYSICAL_CLUSTER);
  }

  public IntegrationTestHarness(int brokersInPhysicalCluster) {
    this.brokersInPhysicalCluster = brokersInPhysicalCluster;
  }

  public PhysicalCluster start(Properties brokerOverrideProps) {
    return start(brokerOverrideProps, Optional.empty());
  }

  public PhysicalCluster start(Properties brokerOverrideProps, Optional<Time> time) {
    physicalCluster = new PhysicalCluster(brokersInPhysicalCluster, brokerOverrideProps, time);
    physicalCluster.start();
    return physicalCluster;
  }

  public void shutdownBrokers() {
    if (physicalCluster != null) {
      physicalCluster.kafkaCluster().shutdownBrokers();
    }
  }

  public void startBrokers() {
    if (physicalCluster != null) {
      physicalCluster.kafkaCluster().startBrokersAfterShutdown();
    }
  }

  public void shutdown() {
    producers.forEach(KafkaProducer::close);
    consumers.forEach(KafkaConsumer::close);
    adminClients.forEach(AdminClient::close);
    if (physicalCluster != null) {
      physicalCluster.shutdown();
    }
  }

  public String zkConnect() {
    return physicalCluster.kafkaCluster().zkConnect();
  }

  public KafkaProducer<String, String> createProducer(LogicalClusterUser user, SecurityProtocol securityProtocol) {
    KafkaProducer<String, String> producer =  KafkaTestUtils.createProducer(
            physicalCluster.bootstrapServers(),
            securityProtocol,
            ScramMechanism.SCRAM_SHA_256.mechanismName(),
            user.saslJaasConfig());
    producers.add(producer);
    return producer;
  }

  public KafkaConsumer<String, String> createConsumer(LogicalClusterUser user, String consumerGroup, SecurityProtocol securityProtocol) {
    KafkaConsumer<String, String> consumer = KafkaTestUtils.createConsumer(
            physicalCluster.bootstrapServers(),
            securityProtocol,
            ScramMechanism.SCRAM_SHA_256.mechanismName(),
            user.saslJaasConfig(),
            consumerGroup);
    consumers.add(consumer);
    return consumer;
  }

  public AdminClient createOAuthAdminClient(String jaasConfig, Properties properties) {
    AdminClient adminClient = KafkaTestUtils.createAdminClient(
            physicalCluster.bootstrapServers(),
            SecurityProtocol.SASL_PLAINTEXT,
            "OAUTHBEARER",
            jaasConfig,
            properties);
    adminClients.add(adminClient);
    return adminClient;
  }

  public AdminClient createAdminClient(LogicalClusterUser user) {
    AdminClient adminClient = KafkaTestUtils.createAdminClient(
        physicalCluster.bootstrapServers(),
        SecurityProtocol.SASL_PLAINTEXT,
        ScramMechanism.SCRAM_SHA_256.mechanismName(),
        user.saslJaasConfig());
    adminClients.add(adminClient);
    return adminClient;
  }

  public void produceConsume(
      LogicalClusterUser producerUser,
      LogicalClusterUser consumerUser,
      String topic,
      String consumerGroup,
      int firstMessageIndex,
      SecurityProtocol securityProtocol)
      throws Throwable {
    String prefixedTopic = producerUser.tenantPrefix() + topic;
    physicalCluster.kafkaCluster().createTopic(prefixedTopic, 2, 1);
    try (KafkaProducer<String, String> producer = createProducer(producerUser, securityProtocol)) {
      KafkaTestUtils.sendRecords(producer, topic, firstMessageIndex, 10);
    }

    try (KafkaConsumer<String, String> consumer = createConsumer(consumerUser, consumerGroup, securityProtocol)) {
      KafkaTestUtils.consumeRecords(consumer, topic, firstMessageIndex, 10);
    }
  }

  public void produceConsume(
          LogicalClusterUser producerUser,
          LogicalClusterUser consumerUser,
          String topic,
          String consumerGroup,
          int firstMessageIndex)
          throws Throwable {
    produceConsume(producerUser, consumerUser, topic, consumerGroup, firstMessageIndex, SecurityProtocol.SASL_PLAINTEXT);
  }

  public AdminClient createPlainAuthAdminClient(String jaasConfig) {
    AdminClient adminClient = KafkaTestUtils.createAdminClient(
        physicalCluster.bootstrapServers(),
        SecurityProtocol.SASL_PLAINTEXT,
        "PLAIN",
        jaasConfig,
        new Properties());
    adminClients.add(adminClient);
    return adminClient;
  }
}
