// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

public class IntegrationTestHarness {

  private PhysicalCluster physicalCluster;
  private final List<KafkaProducer<?, ?>> producers = new ArrayList<>();
  private final List<KafkaConsumer<?, ?>> consumers = new ArrayList<>();
  private final List<AdminClient> adminClients = new ArrayList<>();

  public PhysicalCluster start(Properties brokerOverrideProps) throws Exception {
    physicalCluster = new PhysicalCluster(brokerOverrideProps);
    physicalCluster.start();
    return physicalCluster;
  }

  public void shutdown() throws Exception {
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

  public KafkaProducer<String, String> createProducer(LogicalClusterUser user) {
    KafkaProducer<String, String> producer =  KafkaTestUtils.createProducer(
        physicalCluster.bootstrapServers(),
        SecurityProtocol.SASL_PLAINTEXT,
        ScramMechanism.SCRAM_SHA_256.mechanismName(),
        user.saslJaasConfig());
    producers.add(producer);
    return producer;
  }

  public KafkaConsumer<String, String> createConsumer(LogicalClusterUser user,
      String consumerGroup) {
    KafkaConsumer<String, String> consumer = KafkaTestUtils.createConsumer(
        physicalCluster.bootstrapServers(),
        SecurityProtocol.SASL_PLAINTEXT,
        ScramMechanism.SCRAM_SHA_256.mechanismName(),
        user.saslJaasConfig(),
        consumerGroup);
    consumers.add(consumer);
    return consumer;
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
      int firstMessageIndex)
      throws Throwable {
    String prefixedTopic = producerUser.tenantPrefix() + topic;
    physicalCluster.kafkaCluster().createTopic(prefixedTopic, 2, 1);
    try (KafkaProducer<String, String> producer = createProducer(producerUser)) {
      KafkaTestUtils.sendRecords(producer, topic, firstMessageIndex, 10);
    }

    try (KafkaConsumer<String, String> consumer = createConsumer(consumerUser, consumerGroup)) {
      KafkaTestUtils.consumeRecords(consumer, topic, firstMessageIndex, 10);
    }
  }
}
