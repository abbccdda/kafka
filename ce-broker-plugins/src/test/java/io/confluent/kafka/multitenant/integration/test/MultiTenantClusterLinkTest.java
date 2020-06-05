// (Copyright) [2020 - 2020] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.UserMetadata;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.server.plugins.policy.TopicPolicyConfig;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kafka.log.LogManager;
import kafka.security.authorizer.AclAuthorizer$;
import kafka.server.KafkaConfig$;
import kafka.server.link.ClusterLinkConfig$;
import kafka.server.link.ClusterLinkFactory.LinkManager;
import kafka.server.link.ClusterLinkFetcherManager;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.CreateClusterLinksOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.NewTopicMirror;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.requests.NewClusterLink;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class MultiTenantClusterLinkTest {

  private final MultiTenantCluster sourceCluster = new MultiTenantCluster();
  private final MultiTenantCluster destCluster = new MultiTenantCluster();
  private final String linkName = "tenantLink";
  private final String topic = "linkedTopic";
  private int numPartitions = 2;
  private int nextMessageIndex = 0;


  @Before
  public void setUp() throws Exception {
    sourceCluster.startCluster(brokerProps(false), "sourceLogicalCluster", 1);
    destCluster.startCluster(brokerProps(true), "destLogicalCluster", 11);
    addAcls(destCluster.admin, destCluster.user);
  }

  @After
  public void tearDown() throws Exception {
    sourceCluster.shutdown();
    destCluster.shutdown();
  }

  @Test
  public void testMultiTenantClusterLink() throws Throwable {

    destCluster.createClusterLink(destCluster.admin, linkName, sourceCluster);

    createMirroredTopic();
    verifyTopicMirroring();

    addSourcePartitionsAndVerifyMirror(4);

    changeSourceTopicConfigAndVerifyMirror();

    verifyAclAndOffsetMigration();

    // Verify topic mirroring again after all the updates.
    verifyTopicMirroring();
  }


  @Test
  public void testClusterLinkSecurityUpdate() throws Throwable {

    destCluster.createClusterLink(destCluster.admin, linkName, sourceCluster);
    createMirroredTopic();
    verifyTopicMirroring();
    verifyAclAndOffsetMigration();

    // Update link to use a different user
    LogicalClusterUser newUser = sourceCluster.createUser(2001);
    String newJaasConfig = newUser.saslJaasConfig();
    destCluster.admin.incrementalAlterConfigs(Collections.singletonMap(
        new ConfigResource(Type.CLUSTER_LINK, linkName),
        Collections.singleton(
            new AlterConfigOp(new ConfigEntry(SaslConfigs.SASL_JAAS_CONFIG, newJaasConfig),
                OpType.SET))))
        .all().get(15, TimeUnit.SECONDS);
    waitFor(() -> destCluster.linkConfig(linkName, SaslConfigs.SASL_JAAS_CONFIG), newJaasConfig,
        "Link config not updated");
    sourceCluster.deleteUser(sourceCluster.user);
    sourceCluster.user = newUser;

    verifyTopicMirroring();
    verifyAclAndOffsetMigration();

    // Ensure all metadata updates work with the new user.
    addSourcePartitionsAndVerifyMirror(4);
    changeSourceTopicConfigAndVerifyMirror();
    verifyTopicMirroring();
  }

  private Properties brokerProps(boolean isDest) throws Exception {
    Properties props = new Properties();
    props.put(ConfluentConfigs.CLUSTER_LINK_ENABLE_CONFIG, String.valueOf(isDest));
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
    props.put(AclAuthorizer$.MODULE$.AllowEveryoneIfNoAclIsFoundProp(), "true");
    props.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "1");
    props.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
    props.put(KafkaConfig$.MODULE$.ClientQuotaCallbackClassProp(), TenantQuotaCallback.class.getName());
    props.put(KafkaConfig$.MODULE$.PasswordEncoderSecretProp(), "multi-tenant-cluster-link-secret");
    return props;
  }

  private void createMirroredTopic() throws Exception {
    sourceCluster.physicalCluster.kafkaCluster()
        .createTopic(sourceCluster.user.tenantPrefix() + topic, numPartitions, 1);
    NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.of((short) 1))
        .mirror(Optional.of(new NewTopicMirror(linkName, topic)));
    destCluster.admin.createTopics(Collections.singleton(newTopic)).all().get();
  }

  /**
   * Send some messages to source topic and verify they have been mirrored to destination.
   */
  private void verifyTopicMirroring() throws Throwable {
    int firstMessageIndex = nextMessageIndex;
    int messageCount = 10;
    nextMessageIndex += messageCount;
    KafkaTestUtils.sendRecords(sourceCluster.getOrCreateProducer(), topic, firstMessageIndex, messageCount);
    KafkaTestUtils.consumeRecords(destCluster.getOrCreateConsumer("destGroup"), topic, firstMessageIndex, messageCount);
  }

  /**
   * Add ACLs and commit consumer offsets to source and verify they are migrated to destination.
   */
  private void verifyAclAndOffsetMigration() throws Throwable {
    Set<AclBinding> acls = addAcls(sourceCluster.admin, sourceCluster.user);
    String group = "testGroup";
    Map<TopicPartition, OffsetAndMetadata> offsets = commitOffsets(sourceCluster.admin, group);

    waitFor(() -> destCluster.describeAcls(sourceCluster.user), acls, "Acls not migrated");
    waitFor(() -> destCluster.committedOffsets(group), offsets, "Consumer offsets not migrated");
  }

  /**
   * Add a partition to the source cluster and verify that partitions have been sync'ed.
   */
  private void addSourcePartitionsAndVerifyMirror(int newPartitionCount) throws Exception {
    sourceCluster.admin.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(newPartitionCount)))
        .values().get(topic).get(15, TimeUnit.SECONDS);
    waitFor(() -> destCluster.partitionsForTopic(topic), newPartitionCount, "Topic partitions not updated");
    numPartitions = newPartitionCount;
  }

  /**
   * Change a source topic config and verify that config was updated in destination cluster.
   */
  private void changeSourceTopicConfigAndVerifyMirror() throws Exception {
    sourceCluster.admin.incrementalAlterConfigs(Collections.singletonMap(
        new ConfigResource(Type.TOPIC, topic),
        Collections.singleton(new AlterConfigOp(new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "123456"), OpType.SET))))
        .all().get(15, TimeUnit.SECONDS);
    waitFor(() -> destCluster.topicConfig(topic, TopicConfig.MAX_MESSAGE_BYTES_CONFIG), "123456",
        "Topic configs not migrated");
  }

  /**
   * Add ACLs for specified user to perform operations used in the tests.
   */
  private Set<AclBinding> addAcls(Admin adminClient, LogicalClusterUser user) throws Exception {
    AclBinding topicAcl = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, "linked", PatternType.PREFIXED),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.ALL, AclPermissionType.ALLOW));
    AclBinding clusterAcl = new AclBinding(
        new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    AclBinding groupAcl = new AclBinding(
        new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.READ, AclPermissionType.ALLOW));
    Set<AclBinding> acls = Utils.mkSet(topicAcl, groupAcl, clusterAcl);
    adminClient.createAcls(acls).all().get(15, TimeUnit.SECONDS);
    return acls;
  }

  /**
   * Commit log end offsets for all partitions of the test topic for the provided consumer group.
   */
  private Map<TopicPartition, OffsetAndMetadata> commitOffsets(Admin adminClient, String group) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    LogManager logManager = sourceCluster.physicalCluster.kafkaCluster().brokers().get(0).logManager();
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      TopicPartition prefixedPartition = new TopicPartition(sourceCluster.user.tenantPrefix() + topic, i);
      offsets.put(tp, new OffsetAndMetadata(logManager.getLog(prefixedPartition, false).get().localLogEndOffset()));
    }
    adminClient.alterConsumerGroupOffsets(group, offsets).all().get();
    return offsets;
  }

  private <T> void waitFor(Supplier<T> actual, T expected, String error) throws Exception {
    TestUtils.waitForCondition(() -> expected.equals(actual.get()),
        () -> error + " : expected=" + expected + ", actual=" + actual.get());
  }

  private static class MultiTenantCluster extends IntegrationTestHarness {

    private PhysicalCluster physicalCluster;
    private LogicalCluster logicalCluster;
    private LogicalClusterUser user;
    private ConfluentAdmin admin;
    KafkaProducer<String, String> producer;
    KafkaConsumer<String, String> consumer;

    void startCluster(Properties brokerOverrideProps, String logicalClusterId, int userId) throws Exception {
      physicalCluster = start(brokerOverrideProps, Optional.of(Time.SYSTEM));
      logicalCluster = physicalCluster.createLogicalCluster(logicalClusterId, 100, userId);
      this.user = logicalCluster.user(userId);
      admin = (ConfluentAdmin) super.createAdminClient(logicalCluster.adminUser());
    }

    LogicalClusterUser createUser(int newUserId) {
      UserMetadata newUser = physicalCluster.getOrCreateUser(newUserId, false);
      return logicalCluster.addUser(newUser);
    }

    void deleteUser(LogicalClusterUser user) {
      logicalCluster.removeUser(user.userMetadata.userId());
      deleteAcls(user);
      if (producer != null) {
        producer.close();
        producer = null;
      }
      if (consumer != null) {
        consumer.close();
        consumer = null;
      }
    }

    void createClusterLink(ConfluentAdmin admin, String linkName, MultiTenantCluster sourceCluster) throws Throwable {
      Properties sourceClientProps = KafkaTestUtils.securityProps(
          sourceCluster.physicalCluster.bootstrapServers(),
          SecurityProtocol.SASL_PLAINTEXT,
          ScramMechanism.SCRAM_SHA_256.mechanismName(),
          sourceCluster.user.saslJaasConfig()
      );
      Map<String, String> linkConfigs = new HashMap<>();
      sourceClientProps.stringPropertyNames().forEach(name -> linkConfigs.put(name, sourceClientProps.getProperty(name)));
      linkConfigs.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000");
      linkConfigs.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, "1000");
      linkConfigs.put(ClusterLinkConfig$.MODULE$.TopicConfigSyncMsProp(), "1000");

      String allAclsFilter = "{ \"aclFilters\": [{ \"resourceFilter\": { \"resourceType\": \"any\", \"patternType\": \"any\" },"
          + "\"accessFilter\": { \"operation\": \"any\", \"permissionType\": \"any\" }}]}";
      linkConfigs.put(ClusterLinkConfig$.MODULE$.AclFiltersProp(), allAclsFilter);
      linkConfigs.put(ClusterLinkConfig$.MODULE$.AclSyncEnableProp(), "true");
      linkConfigs.put(ClusterLinkConfig$.MODULE$.AclSyncMsProp(), "2000");


      String allGroupsFilter = "{ \"groupFilters\": [{ \"name\": \"*\", \"patternType\": \"literal\", \"filterType\": \"whitelist\" }]}";
      linkConfigs.put(ClusterLinkConfig$.MODULE$.ConsumerOffsetGroupFiltersProp(), allGroupsFilter);
      linkConfigs.put(ClusterLinkConfig$.MODULE$.ConsumerOffsetSyncEnableProp(), "true");
      linkConfigs.put(ClusterLinkConfig$.MODULE$.ConsumerOffsetSyncMsProp(), "2000");

      NewClusterLink newClusterLink = new NewClusterLink(linkName, sourceCluster.logicalCluster.logicalClusterId(), linkConfigs);
      CreateClusterLinksOptions options = new CreateClusterLinksOptions()
          .validateOnly(false)
          .validateLink(true);
      admin.createClusterLinks(Collections.singleton(newClusterLink), options).all().get();
    }

    KafkaProducer<String, String> getOrCreateProducer() {
      if (producer == null)
        producer = createProducer(user, SecurityProtocol.SASL_PLAINTEXT);
      return producer;
    }

    KafkaConsumer<String, String> getOrCreateConsumer(String groupId) {
      if (consumer == null)
        consumer = createConsumer(user, groupId, SecurityProtocol.SASL_PLAINTEXT);
      return consumer;
    }

    Set<AclBinding> describeAcls(LogicalClusterUser user) {
      try {
        return new HashSet<>(admin.describeAcls(aclBindingFilter(user))
            .values().get(15, TimeUnit.SECONDS));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    void deleteAcls(LogicalClusterUser user) {
      try {
        admin.deleteAcls(Collections.singleton(aclBindingFilter(user)))
            .all().get(15, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private AclBindingFilter aclBindingFilter(LogicalClusterUser user) {
      AccessControlEntryFilter aceFilter = new AccessControlEntryFilter(
          user.unprefixedKafkaPrincipal().toString(),
          null,
          AclOperation.ANY,
          AclPermissionType.ANY);
      return new AclBindingFilter(ResourcePatternFilter.ANY, aceFilter);
    }

    int partitionsForTopic(String topic) {
      try {
        TopicDescription topicDesc = admin.describeTopics(Collections.singleton(topic))
            .values().get(topic).get(15, TimeUnit.SECONDS);
        return topicDesc.partitions().size();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    String topicConfig(String topic, String name) {
      try {
        ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
        Config config = admin.describeConfigs(Collections.singleton(resource))
            .values().get(resource).get(15, TimeUnit.SECONDS);
        return config.entries().stream().filter(e -> e.name().equals(name))
            .findFirst().map(ConfigEntry::value).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Gets link config from the link fetcher manager since we can't get sensitive
     * configs using AdminClient.
     */
    String linkConfig(String linkName, String configName) {
      LinkManager linkManager = physicalCluster.kafkaCluster()
          .brokers().get(0).clusterLinkManager();
      UUID linkId = linkManager.listClusterLinks()
          .find(d -> d.linkName().equals(user.tenantPrefix() + linkName))
          .get().linkId();
      ClusterLinkFetcherManager fetcherManager = linkManager.fetcherManager(linkId).get();
      return fetcherManager.currentConfig().originalsStrings().get(configName);
    }

    Map<TopicPartition, OffsetAndMetadata> committedOffsets(String group) {
      try {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
