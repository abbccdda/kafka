// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.admin.AclCommand;
import kafka.security.auth.Describe$;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class MultiTenantAuthorizerTest {

  private IntegrationTestHarness testHarness;
  private final String topic = "test.topic";
  private final String consumerGroup = "test.consumer.group";
  private PhysicalCluster physicalCluster;
  private LogicalCluster logicalCluster;
  private LogicalClusterUser user1;
  private LogicalClusterUser user2;

  @Before
  public void setUp() throws Exception {
    startTestHarness(brokerProps());
  }

  @After
  public void tearDown() throws Exception {
    testHarness.shutdown();
  }

  private void startTestHarness(Properties brokerOverrideProps) throws Exception {
    testHarness = new IntegrationTestHarness();
    physicalCluster = testHarness.start(brokerOverrideProps);
    logicalCluster = physicalCluster.createLogicalCluster("tenantA", 100, 1, 2);
    user1 = logicalCluster.user(1);
    user2 = logicalCluster.user(2);
  }

  /**
   * Tests LITERAL ACLs for specific resources
   */
  @Test
  public void testLiteralAcls() throws Throwable {
    addProducerAcls(user1, topic, PatternType.LITERAL);
    addConsumerAcls(user2, topic, consumerGroup, PatternType.LITERAL);
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);

    verifyTopicAuthorizationFailure(user1, "sometopic");
    verifyConsumerGroupAuthorizationFailure(user1, topic, "somegroup");
    SecurityTestUtils.verifyAuthorizerLicense(physicalCluster.kafkaCluster(), null);
  }

  /**
   * Tests PREFIXED ACLs for specific resource prefixes.
   */
  @Test
  public void testPrefixedAcls() throws Throwable {
    addProducerAcls(user1, "test", PatternType.PREFIXED);
    addConsumerAcls(user2, "test", "test", PatternType.PREFIXED);
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);

    verifyTopicAuthorizationFailure(user1, "sometopic");
    verifyConsumerGroupAuthorizationFailure(user1, topic, "somegroup");
  }

  /**
   * Tests actual wildcard ACLs in ZooKeeper. These are supported to disable
   * ACLs in some logical clusters or for some users (e.g. broker user).
   * See {@link #testWildcardAclsUsingAdminClient()} for tests which configure
   * per-tenant wildcard ACLs using AdminClient.
   */
  @Test
  public void testWildcardAcls() throws Throwable {
    AclCommand.main(SecurityTestUtils.produceAclArgs(testHarness.zkConnect(),
        user1.prefixedKafkaPrincipal(), "*", PatternType.LITERAL));
    AclCommand.main(SecurityTestUtils.consumeAclArgs(testHarness.zkConnect(),
        user2.prefixedKafkaPrincipal(), "*", "*", PatternType.LITERAL));
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);
  }

  @Test
  public void testSuperUsers() throws Throwable {
    testHarness.produceConsume(logicalCluster.adminUser(), logicalCluster.adminUser(), topic, consumerGroup, 0);
  }

  /**
   * Tests that ACL updates in ZooKeeper are applied for authorization.
   */
  @Test
  public void testAclUpdateInZooKeeper() throws Throwable {
    String topic = "test.topic";
    String consumerGroup = "test.group";
    physicalCluster.kafkaCluster().createTopic(user1.withPrefix(topic), 3, 1);
    try (KafkaConsumer<String, String> consumer = testHarness.createConsumer(user1, consumerGroup, SecurityProtocol.SASL_PLAINTEXT)) {
      assertFalse(checkAuthorized(consumer, topic));
      AclCommand.main(SecurityTestUtils.addTopicAclArgs(testHarness.zkConnect(),
          user1.prefixedKafkaPrincipal(), user1.withPrefix(topic), Describe$.MODULE$, PatternType.LITERAL));
      TestUtils.waitForCondition(() -> checkAuthorized(consumer, topic), "ACL not applied within timeout");
    }
  }

  /**
   * Tests that ACLs are applied within tenant's logical cluster scope.
   */
  @Test
  public void testLogicalClusterScope() throws Throwable {
    addProducerAcls(user1, topic, PatternType.LITERAL);
    addConsumerAcls(user1, topic, consumerGroup, PatternType.LITERAL);
    testHarness.produceConsume(user1, user1, topic, consumerGroup, 0);

    int userId = user1.userMetadata.userId();
    LogicalCluster cluster2 = physicalCluster.createLogicalCluster("anotherCluster", 100, userId);
    LogicalClusterUser cluster2user1 = cluster2.user(userId);
    verifyTopicAuthorizationFailure(cluster2user1, "sometopic");
    verifyConsumerGroupAuthorizationFailure(cluster2user1, topic, "somegroup");

    addProducerAcls(cluster2user1, topic, PatternType.LITERAL);
    addConsumerAcls(cluster2user1, topic, consumerGroup, PatternType.LITERAL);
    testHarness.produceConsume(cluster2user1, cluster2user1, topic, consumerGroup, 0);
  }

  /**
   * Tests LITERAL Acls created using AdminClient with tenant prefix added by interceptor.
   */
  @Test
  public void testLiteralAclsUsingAdminClient() throws Throwable {
    addProducerAclsUsingAdminClient(user1, topic, PatternType.LITERAL);
    addConsumerAclsUsingAdminClient(user2, topic, consumerGroup, PatternType.LITERAL);
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);

    verifyTopicAuthorizationFailure(user1, "sometopic");
    verifyConsumerGroupAuthorizationFailure(user1, topic, "somegroup");
  }

  /**
   * Tests PREFIXED Acls created using AdminClient with tenant prefix added by interceptor.
   */
  @Test
  public void testPrefixAclsUsingAdminClient() throws Throwable {
    addProducerAclsUsingAdminClient(user1, "test.", PatternType.PREFIXED);
    addConsumerAclsUsingAdminClient(user2, "test.", "test.", PatternType.PREFIXED);
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);

    verifyTopicAuthorizationFailure(user1, "sometopic");
    verifyConsumerGroupAuthorizationFailure(user1, topic, "somegroup");
  }

  /**
   * Tests wildcard ACLs configured using AdminClient. These are converted
   * to prefixed ACLs internally.
   */
  @Test
  public void testWildcardAclsUsingAdminClient() throws Throwable {
    addProducerAclsUsingAdminClient(user1, "*", PatternType.LITERAL);
    addConsumerAclsUsingAdminClient(user2, "*", "*", PatternType.LITERAL);
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);
  }

  /**
   * Tests interceptor transformation of CreateAcls/DescribeAcls/DeleteAcls
   * requests and responses using the AdminClient API.
   */
  @Test
  public void testAclCreateDescribeDeleteUsingAdminClient() throws Throwable {
    // Create a non-tenant ACL so that we can test that it is not visible to tenants.
    AclCommand.main(SecurityTestUtils.topicBrokerReadAclArgs(testHarness.zkConnect(), PhysicalCluster.BROKER_PRINCIPAL));
    AdminClient superAdminClient = physicalCluster.superAdminClient();

    LogicalCluster logicalClusterB = physicalCluster.createLogicalCluster("tenantB", 100, 11);
    LogicalClusterUser userB1 = logicalClusterB.user(11);

    AdminClient adminClientA = testHarness.createAdminClient(logicalCluster.adminUser());
    AdminClient adminClientB = testHarness.createAdminClient(logicalClusterB.adminUser());
    ConsumerAcls aclsA = new ConsumerAcls(adminClientA, true);
    ConsumerAcls aclsB = new ConsumerAcls(adminClientB, true);
    aclsA.addAcls(user1, "test1.topic", "test1.group", PatternType.LITERAL);
    aclsA.addAcls(user2, "prefixed.test2", "prefixed.test2", PatternType.PREFIXED);
    aclsB.addAcls(userB1, "*", "*", PatternType.LITERAL);

    physicalCluster.kafkaCluster().createTopic("tenantA_test1.topic", 1, 1);
    physicalCluster.kafkaCluster().createTopic("tenantA_prefixed.test2.topic", 2, 1);
    physicalCluster.kafkaCluster().createTopic("tenantB_test1.topic", 1, 1);

    KafkaConsumer<String, String> consumer1 = testHarness.createConsumer(user1, "test1.group", SecurityProtocol.SASL_PLAINTEXT);
    KafkaConsumer<String, String> consumer2 = testHarness.createConsumer(user2, "test2.group", SecurityProtocol.SASL_PLAINTEXT);
    KafkaConsumer<String, String> consumerB1 = testHarness.createConsumer(userB1, "test1.group", SecurityProtocol.SASL_PLAINTEXT);

    assertTrue(checkAuthorized(consumer1, "test1.topic"));
    assertFalse(checkAuthorized(consumer2, "test1.topic"));
    assertTrue(checkAuthorized(consumer2, "prefixed.test2.topic"));
    assertFalse(checkAuthorized(consumer1, "prefixed.test2.topic"));
    assertTrue(checkAuthorized(consumerB1, "test1.topic"));

    // The describe tests below test various filters. We are ignoring permission type,
    // host etc. since the behaviour for these is unaltered by interceptors.
    // Current ACLs are:
    //    User:broker LITERAL TOPIC *
    //    TenantUser:tenantA_1 LITERAL CLUSTER tenantA_kafka-cluster
    //    TenantUser:tenantA_1 LITERAL GROUP tenantA_test1.group
    //    TenantUser:tenantA_1 LITERAL TOPIC tenantA_test1.topic
    //    TenantUser:tenantA_2 PREFIXED CLUSTER tenantA_kafka-cluster
    //    TenantUser:tenantA_2 PREFIXED GROUP tenantA_prefixed.test2
    //    TenantUser:tenantA_2 PREFIXED TOPIC tenantA_prefixed.test2
    //    TenantUser:tenantB_11 LITERAL CLUSTER tenantB_kafka-cluster
    //    TenantUser:tenantB_11 PREFIXED GROUP tenantB_
    //    TenantUser:tenantB_11 PREFIXED TOPIC tenantB_

    // Base ACLs from ZooKeeper are obtained using the internal listener whose requests don't
    // get intercepted. So these contain matching ACLs of all tenants exactly as stored in ZK.
    ConsumerAcls baseAcls = new ConsumerAcls(superAdminClient, false);
    baseAcls.verifyAllAcls(null, PatternType.ANY);
    baseAcls.verifyAllAcls(null, PatternType.MATCH);
    baseAcls.verifyAcls(ResourceType.TOPIC, null, PatternType.LITERAL, null, "*", "tenantA_test1.topic");
    baseAcls.verifyAcls(ResourceType.GROUP, null, PatternType.PREFIXED, null,
        "tenantA_prefixed.test2", "tenantB_");
    baseAcls.verifyAcls(ResourceType.ANY, null, PatternType.LITERAL, null,
        "*", "tenantA_test1.topic", "tenantA_test1.group", "tenantA_kafka-cluster", "tenantB_kafka-cluster");
    baseAcls.verifyAcls(ResourceType.TOPIC, null, PatternType.ANY, null,
        "*", "tenantA_test1.topic", "tenantA_prefixed.test2", "tenantB_");


    aclsA.verifyAllAcls(null, PatternType.ANY);
    aclsA.verifyAllAcls(null, PatternType.MATCH);
    aclsA.verifyAcls(ResourceType.TOPIC, null, PatternType.LITERAL, null,
        "test1.topic");
    aclsA.verifyAcls(ResourceType.GROUP, null, PatternType.PREFIXED, null,
        "prefixed.test2");
    aclsA.verifyAcls(ResourceType.ANY, null, PatternType.LITERAL, null,
        "test1.topic", "test1.group", "kafka-cluster");
    aclsA.verifyAcls(ResourceType.ANY, null, PatternType.LITERAL, user1,
        "test1.topic", "test1.group", "kafka-cluster");
    aclsA.verifyAcls(ResourceType.TOPIC, null, PatternType.ANY, null,
        "test1.topic", "prefixed.test2");

    aclsB.verifyAllAcls(null, PatternType.ANY);
    aclsB.verifyAcls(ResourceType.TOPIC, "*", PatternType.LITERAL, userB1, "*");
    aclsB.verifyAcls(ResourceType.TOPIC, "*", PatternType.ANY, userB1, "*");
    aclsB.verifyAcls(ResourceType.TOPIC, "*", PatternType.PREFIXED, userB1);
    aclsB.verifyAcls(ResourceType.TOPIC, null, PatternType.LITERAL, userB1, "*");
    aclsB.verifyAcls(ResourceType.ANY, null, PatternType.LITERAL, null, "*", "kafka-cluster");
    aclsB.verifyAcls(ResourceType.TOPIC, null, PatternType.PREFIXED, userB1);
    aclsB.verifyAcls(ResourceType.ANY, "kafka-cluster", PatternType.LITERAL, userB1, "kafka-cluster");

    baseAcls.verifyAcls(ResourceType.ANY, "tenantA_prefixed.test2.topic", PatternType.MATCH, null,
        "tenantA_prefixed.test2", "*");
    baseAcls.verifyAcls(ResourceType.TOPIC, null, PatternType.MATCH, null,
        "tenantA_test1.topic", "tenantA_prefixed.test2", "tenantB_", "*");
    aclsA.verifyAcls(ResourceType.ANY, "prefixed.test2.topic", PatternType.MATCH, null, "prefixed.test2");
    aclsA.verifyAcls(ResourceType.ANY, "prefixed.test2.topic", PatternType.MATCH, user2, "prefixed.test2");
    aclsA.verifyAcls(ResourceType.TOPIC, null, PatternType.MATCH, user1, "test1.topic");
    aclsB.verifyAcls(ResourceType.TOPIC, "test", PatternType.MATCH, userB1, "*");
    aclsB.verifyAcls(ResourceType.TOPIC, "*", PatternType.MATCH, userB1, "*");
    aclsB.verifyAcls(ResourceType.TOPIC, null, PatternType.MATCH, null, "*");

    assertTrue(checkAuthorized(consumer1, "test1.topic"));
    aclsA.deleteAcls(ResourceType.TOPIC, "test1.topic", PatternType.LITERAL, null);
    assertFalse(checkAuthorized(consumer1, "test1.topic"));
    aclsA.verifyAllAcls(null, PatternType.ANY);

    assertTrue(checkAuthorized(consumer2, "prefixed.test2.topic"));
    aclsA.deleteAcls(ResourceType.TOPIC, "prefixed.test2.topic", PatternType.MATCH, user2);
    assertFalse(checkAuthorized(consumer2, "prefixed.test2.topic"));
    aclsA.verifyAllAcls(null, PatternType.ANY);

    assertTrue(checkAuthorized(consumerB1, "test1.topic"));
    aclsB.deleteAcls(ResourceType.TOPIC, null, PatternType.MATCH, userB1);
    assertFalse(checkAuthorized(consumerB1, "test1.topic"));
    aclsB.verifyAllAcls(null, PatternType.MATCH);
  }

  /**
   * Tests per-tenant ACL limit
   */
  @Test
  public void testAclLimit() throws Throwable {
    addProducerAcls(user1, topic, PatternType.LITERAL);
    addConsumerAcls(user2, topic, consumerGroup, PatternType.LITERAL);

    Function<String, AclBinding> topicAcl = topic -> new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(user1.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.WRITE, AclPermissionType.ALLOW));

    try (AdminClient adminClient = testHarness.createAdminClient(logicalCluster.adminUser())) {
      int aclCount =  adminClient.describeAcls(new AclBindingFilter(
          new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
          new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
      )).values().get().size();
      for (int i = 0; i < 100 - aclCount; i++) {
        adminClient.createAcls(Collections.singleton(topicAcl.apply("topic" + i))).all().get();
      }
      try {
        adminClient.createAcls(Collections.singleton(topicAcl.apply("othertopic"))).all().get();
      } catch (ExecutionException e) {
        assertEquals(InvalidRequestException.class, e.getCause().getClass());
      }
    }

    // We should still be able to create ACLs for other tenants
    LogicalCluster logicalCluster2 = physicalCluster.createLogicalCluster("anotherCluster", 100);
    try (AdminClient adminClient = testHarness.createAdminClient(logicalCluster2.adminUser())) {
      adminClient.createAcls(Collections.singleton(topicAcl.apply("sometopic"))).all().get();
    }

    // ACLs created before limit was reached should work
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);
  }

  /**
   * Tests that per-tenant ACL limit of zero disables authorization
   */
  @Test
  public void testAuthorizerDisabledUsingAclLimit() throws Throwable {
    testHarness.shutdown();
    Properties brokerProps = brokerProps();
    brokerProps.put(MultiTenantAuthorizer.MAX_ACLS_PER_TENANT_PROP, "0");
    startTestHarness(brokerProps);

    AclBinding topicAcl = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(user1.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.WRITE, AclPermissionType.ALLOW));
    AclBindingFilter topicFilter = new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

    try (AdminClient adminClient = testHarness.createAdminClient(logicalCluster.adminUser())) {
      try {
        adminClient.createAcls(Collections.singleton(topicAcl)).all().get();
      } catch (ExecutionException e) {
        verifyAclsDisabledException(e);
      }
      try {
        adminClient.describeAcls(topicFilter).values().get();
      } catch (ExecutionException e) {
        verifyAclsDisabledException(e);
      }
      try {
        adminClient.deleteAcls(Collections.singleton(topicFilter)).all().get();
      } catch (ExecutionException e) {
        verifyAclsDisabledException(e);
      }
    }

    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);
  }

  private void verifyAclsDisabledException(ExecutionException e) {
    Throwable cause = e.getCause();
    assertTrue("Unexpected exception: " + cause, cause instanceof InvalidRequestException);
  }

  @Test
  public void testInvalidAcl() throws Throwable {
    try (AdminClient adminClient = testHarness.createAdminClient(logicalCluster.adminUser())) {
      List<String> invalidPrincipals = Arrays.asList("", "userWithoutPrincipalType");
      invalidPrincipals.forEach(principal -> {
        AclBinding acl = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
            new AccessControlEntry(principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW));
        try {
          adminClient.createAcls(Collections.singleton(acl)).all().get();
          fail("createAcls didn't fail with invalid principal");
        } catch (Exception e) {
          assertTrue("Invalid exception: " + e, e instanceof ExecutionException
              && e.getCause() instanceof InvalidRequestException);
        }
      });
    }
  }

  private Properties brokerProps() {
    Properties props = new Properties();
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), MultiTenantAuthorizer.class.getName());
    props.put(MultiTenantAuthorizer.MAX_ACLS_PER_TENANT_PROP, "100");
    return props;
  }

  private class ConsumerAcls {
    private final AdminClient adminClient;
    private final Set<AclBinding> acls;
    private final boolean tenantOnly;

    ConsumerAcls(AdminClient adminClient, boolean tenantOnly) throws Exception {
      this.adminClient = adminClient;
      this.tenantOnly = tenantOnly;
      this.acls = new HashSet<>();
      this.acls.addAll(describeAcls(null, PatternType.ANY));
    }

    private void addAcls(LogicalClusterUser user, String topic, String consumerGroup,
        PatternType patternType) throws Exception {
      List<AclBinding> consumerAcls = consumerAcls(user, topic, consumerGroup, patternType);
      adminClient.createAcls(consumerAcls).all().get();
      this.acls.addAll(consumerAcls);
    }

    private void deleteAcls(ResourceType resourceType, String resourceName, PatternType patternType,
        LogicalClusterUser user) throws Exception {
      String principal = user == null ? null : user.unprefixedKafkaPrincipal().toString();
      Collection<AclBinding> deletedAcls = adminClient.deleteAcls(Collections.singletonList(
          new AclBindingFilter(
              new ResourcePatternFilter(resourceType, resourceName, patternType),
              new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY)
          ))).all().get();
      this.acls.removeAll(deletedAcls);
    }

    private Set<AclBinding> describeAcls(String resourceName, PatternType patternType) throws Exception {
      Collection<AclBinding> describedAcls =  adminClient.describeAcls(new AclBindingFilter(
          new ResourcePatternFilter(ResourceType.ANY, resourceName, patternType),
          new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
      )).values().get();
      return new HashSet<>(describedAcls);
    }

    private void verifyAllAcls(String resourceName, PatternType patternType) throws Exception {
      Set<AclBinding> describedAcls = describeAcls(resourceName, patternType);
      assertEquals(this.acls, describedAcls);
    }

    private void verifyAcls(ResourceType resourceType, String resourceName,
        PatternType patternType, LogicalClusterUser user, String... expectedResources) throws Exception {
      String principal = user == null ? null : user.unprefixedKafkaPrincipal().toString();
      Collection<AclBinding> acls = adminClient.describeAcls(new AclBindingFilter(
          new ResourcePatternFilter(resourceType, resourceName, patternType),
          new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY)
      )).values().get();
      Set<String> aclResources = acls.stream().map(acl -> acl.pattern().name()).collect(Collectors.toSet());
      assertEquals(Utils.mkSet(expectedResources), aclResources);
      if (tenantOnly) {
        acls.forEach(acl -> assertFalse("Unexpected acl " + acl,
            acl.entry().principal().contains(PhysicalCluster.BROKER_PRINCIPAL.getName())));
      }
    }
  }

  private void addProducerAcls(LogicalClusterUser user, String topic, PatternType patternType) {
    AclCommand.main(SecurityTestUtils.produceAclArgs(testHarness.zkConnect(),
        user.prefixedKafkaPrincipal(), user.withPrefix(topic), patternType));
  }

  private void addConsumerAcls(LogicalClusterUser user, String topic, String consumerGroup,
      PatternType patternType) {
    AclCommand.main(SecurityTestUtils.consumeAclArgs(testHarness.zkConnect(),
        user.prefixedKafkaPrincipal(), user.withPrefix(topic), user.withPrefix(consumerGroup),
        patternType));
  }

  private void addProducerAclsUsingAdminClient(LogicalClusterUser user, String topic,
      PatternType patternType) throws Exception {
    try (AdminClient adminClient = testHarness.createAdminClient(logicalCluster.adminUser())) {
      addProducerAcls(adminClient, user, topic, patternType);
    }
  }

  private void addProducerAcls(AdminClient adminClient, LogicalClusterUser user, String topic,
      PatternType patternType) throws Exception {
    AclBinding topicAcl = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, patternType),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.WRITE, AclPermissionType.ALLOW));
    AclBinding clusterAcl = new AclBinding(
        new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", patternType),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    adminClient.createAcls(Arrays.asList(topicAcl, clusterAcl)).all().get();
  }

  private void addConsumerAclsUsingAdminClient(LogicalClusterUser user, String topic, String consumerGroup,
      PatternType patternType) throws Exception {
    try (AdminClient adminClient = testHarness.createAdminClient(logicalCluster.adminUser())) {
      addConsumerAcls(adminClient, user, topic, consumerGroup, patternType);
    }
  }
  private void addConsumerAcls(AdminClient adminClient,
      LogicalClusterUser user, String topic, String consumerGroup,
      PatternType patternType) throws Exception {

    List<AclBinding> acls = consumerAcls(user, topic, consumerGroup, patternType);
    adminClient.createAcls(acls).all().get();
  }

  private List<AclBinding> consumerAcls(LogicalClusterUser user, String topic, String consumerGroup,
      PatternType patternType) {
    AclBinding topicAcl = new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, patternType),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.READ, AclPermissionType.ALLOW));
    AclBinding consumerGroupAcl = new AclBinding(
        new ResourcePattern(ResourceType.GROUP, consumerGroup, patternType),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.ALL, AclPermissionType.ALLOW));
    AclBinding clusterAcl = new AclBinding(
        new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", patternType),
        new AccessControlEntry(user.unprefixedKafkaPrincipal().toString(),
            "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    return Arrays.asList(topicAcl, consumerGroupAcl, clusterAcl);
  }

  private void verifyTopicAuthorizationFailure(LogicalClusterUser user, String topic) {
    try (KafkaProducer<String, String> producer = testHarness.createProducer(user, SecurityProtocol.SASL_PLAINTEXT)) {
      try {
        producer.partitionsFor(topic);
        fail("Authorization should have failed");
      } catch (AuthorizationException e) {
        // Expected exception
      }
    }
  }

  private void verifyConsumerGroupAuthorizationFailure(LogicalClusterUser user,
      String topic,
      String group) {

    try (KafkaConsumer<String, String> consumer = testHarness.createConsumer(user, group, SecurityProtocol.SASL_PLAINTEXT)) {
      consumer.subscribe(Collections.singleton(topic));
      consumer.poll(Duration.ofSeconds(5));
      fail("Authorization should have failed");
    } catch (AuthorizationException e) {
      // Expected exception
    }
  }

  private boolean checkAuthorized(KafkaConsumer<?, ?> consumer, String topic) {
    try {
      consumer.partitionsFor(topic).size();
      return true;
    } catch (AuthorizationException e) {
      return false;
    }
  }
}
