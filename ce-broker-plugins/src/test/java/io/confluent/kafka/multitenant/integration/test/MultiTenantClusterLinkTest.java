// (Copyright) [2020 - 2020] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.server.plugins.policy.TopicPolicyConfig;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kafka.security.authorizer.AclAuthorizer$;
import kafka.server.KafkaConfig$;
import kafka.server.link.ClusterLinkConfig$;
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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.requests.NewClusterLink;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
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

  @Before
  public void setUp() throws Exception {
    sourceCluster.startCluster(brokerProps(false), "sourceLogicalCluster", 1);
    destCluster.startCluster(brokerProps(true), "destLogicalCluster", 11);
  }

  @After
  public void tearDown() throws Exception {
    sourceCluster.shutdown();
    destCluster.shutdown();
  }

  @Test
  public void testTopicMirror() throws Throwable {

    // Create cluster link and mirror one topic
    destCluster.createClusterLink(destCluster.admin, linkName, sourceCluster);

    sourceCluster.physicalCluster.kafkaCluster().createTopic(sourceCluster.user.tenantPrefix() + topic, 2, 1);
    NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.of((short) 1))
        .linkName(Optional.of(linkName))
        .mirrorTopic(Optional.of(topic));
    destCluster.admin.createTopics(Collections.singleton(newTopic)).all().get();

    // Verify mirroring
    KafkaProducer<String, String> sourceProducer = sourceCluster.createProducer();
    KafkaTestUtils.sendRecords(sourceProducer, topic, 0, 10);
    KafkaConsumer<String, String> destConsumer = destCluster.createConsumer("destGroup");
    KafkaTestUtils.consumeRecords(destConsumer, topic, 0, 10);

    // Add a partition to the source cluster and verify that partitions have been sync'ed
    sourceCluster.admin.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(4)))
        .values().get(topic).get(15, TimeUnit.SECONDS);
    waitFor(() -> destCluster.partitionsForTopic(topic), 4, "Topic partitions not updated");

    // Change a source config and verify that config was updated in destination cluster
    sourceCluster.admin.incrementalAlterConfigs(Collections.singletonMap(
        new ConfigResource(Type.TOPIC, topic),
        Collections.singleton(new AlterConfigOp(new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "123456"), OpType.SET))))
        .all().get(15, TimeUnit.SECONDS);
    waitFor(() -> destCluster.topicConfig(topic, TopicConfig.MAX_MESSAGE_BYTES_CONFIG), "123456",
        "Topic configs not migrated");

    // Verify mirroring
    KafkaTestUtils.sendRecords(sourceProducer, topic, 10, 20);
    KafkaTestUtils.consumeRecords(destConsumer, topic, 10, 20);
  }

  @Test
  public void testAclMigration() throws Throwable {
    destCluster.createClusterLink(destCluster.admin, linkName, sourceCluster);

    Collection<AclBinding> acls = addAcls(sourceCluster.admin, sourceCluster.user);
    waitFor(destCluster::describeAcls, acls, "Acls not migrated");
  }

  private Properties brokerProps(boolean isDest) throws Exception {
    Properties props = new Properties();
    props.put(ConfluentConfigs.CLUSTER_LINK_ENABLE_CONFIG, String.valueOf(isDest));
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
    props.put(AclAuthorizer$.MODULE$.AllowEveryoneIfNoAclIsFoundProp(), "true");
    props.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "1");
    props.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
    props.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
    props.put(KafkaConfig$.MODULE$.ClientQuotaCallbackClassProp(), TenantQuotaCallback.class.getName());
    return props;
  }

  private Collection<AclBinding> addAcls(Admin adminClient, LogicalClusterUser user) throws Exception {
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
            "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    Set<AclBinding> acls = Utils.mkSet(topicAcl, groupAcl, clusterAcl);
    adminClient.createAcls(acls).all().get(15, TimeUnit.SECONDS);
    return acls;
  }

  private <T> void waitFor(Supplier<T> actual, T expected, String error) throws Exception {
    TestUtils.waitForCondition(() -> expected.equals(actual.get()),
        () -> error + " : " + actual.get());
  }

  private static class MultiTenantCluster extends IntegrationTestHarness {

    private PhysicalCluster physicalCluster;
    private LogicalCluster logicalCluster;
    private LogicalClusterUser user;
    private ConfluentAdmin admin;

    void startCluster(Properties brokerOverrideProps, String logicalClusterId, int userId) throws Exception {
      physicalCluster = start(brokerOverrideProps, Optional.of(Time.SYSTEM));
      logicalCluster = physicalCluster.createLogicalCluster(logicalClusterId, 100, userId);
      this.user = logicalCluster.user(userId);
      admin = (ConfluentAdmin) super.createAdminClient(logicalCluster.adminUser());
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

      NewClusterLink newClusterLink = new NewClusterLink(linkName, sourceCluster.logicalCluster.logicalClusterId(), linkConfigs);
      CreateClusterLinksOptions options = new CreateClusterLinksOptions()
          .validateOnly(false)
          .validateLink(true);
      admin.createClusterLinks(Collections.singleton(newClusterLink), options).all().get();
    }

    KafkaProducer<String, String> createProducer() {
      return createProducer(user, SecurityProtocol.SASL_PLAINTEXT);
    }

    KafkaConsumer<String, String> createConsumer(String groupId) {
      return createConsumer(user, groupId, SecurityProtocol.SASL_PLAINTEXT);
    }

    Set<AclBinding> describeAcls() {
      try {
        return new HashSet<>(admin.describeAcls(AclBindingFilter.ANY).values().get(15, TimeUnit.SECONDS));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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
  }
}
