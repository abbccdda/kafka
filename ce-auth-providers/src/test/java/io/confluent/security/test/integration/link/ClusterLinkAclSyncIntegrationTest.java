/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.security.test.integration.link;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.test.utils.RbacClusters;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.server.KafkaConfig$;
import kafka.server.link.ClusterLinkConfig$;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.CreateClusterLinksOptions;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.requests.NewClusterLink;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ClusterLinkAclSyncIntegrationTest {

  private static final String BROKER_USER = "kafka";
  private static final String DEVELOPER1 = "app1-developer";
  private static final String APP1_TOPIC = "app1-topic";
  private static final String APP1_CONSUMER_GROUP = "app1-consumer-group";

  private final String allAclsFilter =
      "{ \"aclFilters\": [{ \"resourceFilter\": { \"resourceType\": \"any\","
          + " \"patternType\": \"any\" },\"accessFilter\": { \"operation\": \"any\","
          + " \"permissionType\": \"any\" }}]}";
  private final String subsetAclsFilter =
      "{ \"aclFilters\": [{ \"resourceFilter\": { \"resourceType\": \"topic\", \"name\":"
          + " \"app1-topic\", \"patternType\": \"literal\" },\"accessFilter\": { \"principal\":"
          + " \"User:app1-developer\", \"operation\": \"write\", \"host\": \"*\","
          + " \"permissionType\": \"allow\" }}]}";

  private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);
  private final Collection<AclBinding> allExpectedAcls = Arrays.asList(new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, APP1_TOPIC, PatternType.LITERAL),
      new AccessControlEntry(principal.toString(),
          "*", AclOperation.WRITE, AclPermissionType.ALLOW)), new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, APP1_TOPIC, PatternType.LITERAL),
      new AccessControlEntry(principal.toString(),
          "*", AclOperation.READ, AclPermissionType.ALLOW)), new AclBinding(
      new ResourcePattern(ResourceType.GROUP, APP1_CONSUMER_GROUP, PatternType.LITERAL),
      new AccessControlEntry(principal.toString(),
          "*", AclOperation.ALL, AclPermissionType.ALLOW)));
  private final Collection<AclBinding> subsetExpectedAcls = Collections.singletonList(
      new AclBinding(new ResourcePattern(ResourceType.TOPIC, APP1_TOPIC, PatternType.LITERAL),
          new AccessControlEntry(principal.toString(),
              "*", AclOperation.WRITE, AclPermissionType.ALLOW)));

  private RbacClusters.Config rbacConfig;
  private RbacClusters sourceCluster;
  private RbacClusters destCluster;

  @Before
  public void setUp() {
    List<String> otherUsers = Collections.singletonList(DEVELOPER1);
    rbacConfig = new RbacClusters.Config().users(BROKER_USER, otherUsers);
  }

  @After
  public void tearDown() {
    try {
      if (sourceCluster != null) {
        sourceCluster.shutdown();
      }
      if (destCluster != null) {
        destCluster.shutdown();
      }
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  @Test
  public void testZkBasedAclTotalMigrationForClusterLinking() throws Throwable {
    // configure clusters to use ZK based ACLs
    rbacConfig = rbacConfig
        .overrideBrokerConfig(ConfluentConfigs.CLUSTER_LINK_ENABLE_CONFIG, "true")
        .overrideBrokerConfig(KafkaConfig$.MODULE$.PasswordEncoderSecretProp(), "link-secret")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ZK_ACL")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.MIGRATE_ACLS_FROM_ZK_PROP, "false");

    // verify all ACLs are being migrated
    verifyAclMigrationClusterLinking(allAclsFilter, allExpectedAcls);
  }

  @Test
  public void testCentralizedAclTotalMigrationForClusterLinking() throws Throwable {
    // configure clusters to use centralized ACLs
    rbacConfig = rbacConfig
        .overrideBrokerConfig(ConfluentConfigs.CLUSTER_LINK_ENABLE_CONFIG, "true")
        .overrideBrokerConfig(KafkaConfig$.MODULE$.PasswordEncoderSecretProp(), "link-secret")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT")
        .overrideMetadataBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP,
            "CONFLUENT")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.MIGRATE_ACLS_FROM_ZK_PROP, "false");

    // verify all ACLs are being migrated
    verifyAclMigrationClusterLinking(allAclsFilter, allExpectedAcls);
  }

  @Test
  public void testZkBasedAclSubsetMigrationForClusterLinking() throws Throwable {
    // configure clusters to use ZK based ACLs
    rbacConfig = rbacConfig
        .overrideBrokerConfig(ConfluentConfigs.CLUSTER_LINK_ENABLE_CONFIG, "true")
        .overrideBrokerConfig(KafkaConfig$.MODULE$.PasswordEncoderSecretProp(), "link-secret")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ZK_ACL")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.MIGRATE_ACLS_FROM_ZK_PROP, "false");

    // verify only subset of ACLs are being migrated
    verifyAclMigrationClusterLinking(subsetAclsFilter, subsetExpectedAcls);
  }

  @Test
  public void testCentralizedAclSubsetMigrationForClusterLinking() throws Throwable {
    // configure clusters to use centralized ACLs
    rbacConfig = rbacConfig
        .overrideBrokerConfig(ConfluentConfigs.CLUSTER_LINK_ENABLE_CONFIG, "true")
        .overrideBrokerConfig(KafkaConfig$.MODULE$.PasswordEncoderSecretProp(), "link-secret")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT")
        .overrideMetadataBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP,
            "CONFLUENT")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.MIGRATE_ACLS_FROM_ZK_PROP, "false");

    // verify only subset of ACLs are being migrated
    verifyAclMigrationClusterLinking(subsetAclsFilter, subsetExpectedAcls);
  }

  private void verifyAclMigrationClusterLinking(String aclFilter,
      Collection<AclBinding> expectedAcls) throws Throwable {
    // initializing source cluster
    sourceCluster = new RbacClusters(rbacConfig);

    // adding produce and consumer ACLs for APP1_TOPIC
    ClientBuilder srcClientBuilder = sourceCluster.clientBuilder(BROKER_USER);
    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);
    KafkaTestUtils.addProducerAcls(srcClientBuilder, principal, APP1_TOPIC, PatternType.LITERAL);
    KafkaTestUtils.addConsumerAcls(srcClientBuilder, principal, APP1_TOPIC, APP1_CONSUMER_GROUP,
        PatternType.LITERAL);

    // getting cluster link properties
    Properties sourceClientProps = KafkaTestUtils.securityProps(
        sourceCluster.kafkaCluster.bootstrapServers(),
        sourceCluster.kafkaSecurityProtocol,
        sourceCluster.kafkaSaslMechanism,
        sourceCluster.users.get(BROKER_USER).jaasConfig
    );
    Map<String, String> linkConfigs = new HashMap<>();
    sourceClientProps.stringPropertyNames().forEach(name -> linkConfigs.put(name,
        sourceClientProps.getProperty(name)));
    linkConfigs.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    linkConfigs.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, "1000");
    linkConfigs.put(ClusterLinkConfig$.MODULE$.AclFiltersProp(), aclFilter);
    linkConfigs.put(ClusterLinkConfig$.MODULE$.AclSyncEnableProp(), "true");
    linkConfigs.put(ClusterLinkConfig$.MODULE$.AclSyncMsProp(), "1000");
    NewClusterLink newClusterLink = new NewClusterLink("testLink",
        sourceCluster.kafkaClusterId(), linkConfigs);
    CreateClusterLinksOptions options = new CreateClusterLinksOptions()
        .validateOnly(false)
        .validateLink(true);

    // initializing destination cluster
    destCluster = new RbacClusters(rbacConfig);
    ClientBuilder destClientBuilder = destCluster.clientBuilder(BROKER_USER);
    try (ConfluentAdmin admin = (ConfluentAdmin) destClientBuilder.buildAdminClient()) {
      // create cluster link
      admin.createClusterLinks(Collections.singleton(newClusterLink), options).all().get();

      // verify that ACLs migrated
      TestUtils.waitForCondition(() -> CollectionUtils.isEqualCollection(
          admin.describeAcls(AclBindingFilter.ANY).values().get(), expectedAcls),
          "ACLs not migrated");
    }
  }
}
