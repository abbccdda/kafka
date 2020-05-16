/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.kafka.multitenant.integration.test;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.common.resource.Resource.CLUSTER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.multitenant.authorizer.MultiTenantAuditLogConfig;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.LogicalClusterUser;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;
import io.confluent.kafka.security.authorizer.MockAuditLogProvider;
import io.confluent.kafka.test.cluster.EmbeddedKafka;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.authorizer.AclAccessRule;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.admin.AclCommand;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class MultiTenantAuditLogTest {

  private IntegrationTestHarness testHarness;
  private final String topic = "test.topic";
  private final String consumerGroup = "test.consumer.group";
  private final String logicalClusterId = "lkc-1234";
  private PhysicalCluster physicalCluster;
  private LogicalCluster logicalCluster;
  private LogicalClusterUser user1;
  private LogicalClusterUser user2;

  @Before
  public void setUp() throws Exception {
    MockAuditLogProvider.reset();
  }

  @After
  public void tearDown() throws Exception {
    testHarness.shutdown();
  }

  private void startTestHarness(Properties brokerOverrideProps) throws Exception {
    testHarness = new IntegrationTestHarness();
    physicalCluster = testHarness.start(brokerOverrideProps);
    logicalCluster = physicalCluster.createLogicalCluster(logicalClusterId, 100, 1, 2);
    user1 = logicalCluster.user(1);
    user2 = logicalCluster.user(2);
    TestUtils.waitForCondition(this::auditLoggerReady, 10000, "Audit Logger Ready");
  }

  private boolean auditLoggerReady() {
    try {
      if (physicalCluster.kafkaCluster().kafkas().isEmpty()) {
        return false;
      }
      for (EmbeddedKafka broker : physicalCluster.kafkaCluster().kafkas()) {
        MultiTenantAuthorizer authorizer =
            (MultiTenantAuthorizer) broker.kafkaServer().authorizer().get();
        if (!authorizer.isAuditLogEnabled()) {
          return true;
        }
      }
      return true;
    } catch (ClassCastException e) {
      return false;
    }
  }


  @Test
  public void testDisabled() throws Throwable {
    startTestHarness(brokerProps(false));
    addProducerAcls(user1, topic, PatternType.LITERAL);
    addConsumerAcls(user2, topic, consumerGroup, PatternType.LITERAL);
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);

    assertTrue(MockAuditLogProvider.instance.auditLog.isEmpty());
  }

  @Test
  public void testLiteralAcls() throws Throwable {
    startTestHarness(brokerProps(true));
    // In an ideal world these would be a series of separate tests, but we'd like to avoid
    // the expense of setting up the clusters in the test harness over and over
    addProducerAcls(user1, topic, PatternType.LITERAL);
    addConsumerAcls(user2, topic, consumerGroup, PatternType.LITERAL);
    testHarness.produceConsume(user1, user2, topic, consumerGroup, 0);

    // for debugging
    List<ConfluentAuthorizationEvent> user1s = MockAuditLogProvider.instance.auditLog.stream()
        .filter(e -> e.requestContext().principal().toString().equals("User:1")
        ).collect(toList());

    // make sure we have an appropriate produce event
    List<ConfluentAuthorizationEvent> produces = MockAuditLogProvider.instance.auditLog.stream()
        .filter(e -> e.requestContext().principal().toString().equals("User:1") &&
            e.action().resourceName().equals(topic) &&
            e.action().operation().name().equals("Write") &&
            e.authorizePolicy() instanceof AclAccessRule &&
            ((AclAccessRule) e.authorizePolicy()).resourcePattern().name().equals(topic)
        ).collect(toList());
    assertEquals(1, produces.size());

    // make sure we have at least one appropriate consume event (probably multiple because
    // message might not be delivered for the first)
    List<ConfluentAuthorizationEvent> topicReads = MockAuditLogProvider.instance.auditLog.stream()
        .filter(e -> e.requestContext().principal().toString().equals("User:2") &&
            e.action().resourceName().equals(topic) &&
            e.action().operation().name().equals("Read") &&
            e.authorizePolicy() instanceof AclAccessRule &&
            ((AclAccessRule) e.authorizePolicy()).resourcePattern().name().equals(topic)
        ).collect(toList());
    assertFalse(topicReads.isEmpty());

    List<ConfluentAuthorizationEvent> groupReads = MockAuditLogProvider.instance.auditLog.stream()
        .filter(e -> e.requestContext().principal().toString().equals("User:2") &&
            e.action().resourceName().equals(consumerGroup) &&
            e.action().operation().name().equals("Read") &&
            e.authorizePolicy() instanceof AclAccessRule &&
            ((AclAccessRule) e.authorizePolicy()).resourcePattern().name().equals(consumerGroup)
        ).collect(toList());
    assertFalse(groupReads.isEmpty());

    // make sure we have no events that refer to TenantUsers
    assertFalse(
        MockAuditLogProvider.instance.auditLog.stream()
            .anyMatch(e -> e.requestContext().principal().toString().contains("TenantUser:")));

    // make sure that for all events for Tenant users, all of the other information is correct
    List<ConfluentAuthorizationEvent> tenantUserEntries = MockAuditLogProvider.instance.auditLog.stream()
        .filter(e -> e.requestContext().principal().toString().equals("User:1") ||
            e.requestContext().principal().toString().equals("User:2"))
        .collect(toList());

    assertTrue(
        tenantUserEntries.stream()
            .allMatch(e -> e.sourceScope().equals(Scope.kafkaClusterScope(logicalClusterId))));

    assertTrue(
        tenantUserEntries.stream()
            .allMatch(e -> e.sourceScope().equals(Scope.kafkaClusterScope(logicalClusterId))));

    assertTrue(
        tenantUserEntries.stream()
            .allMatch(e -> e.action().scope().equals(Scope.kafkaClusterScope(logicalClusterId))));

    assertTrue(
        tenantUserEntries.stream()
            .allMatch(e -> e.authorizePolicy() instanceof AclAccessRule &&
                // Should *not* be tenant-prefixed
                !((AclAccessRule) e.authorizePolicy()).resourcePattern().name()
                    .contains(logicalClusterId) &&
                !((AclAccessRule) e.authorizePolicy()).aclBinding().entry().principal()
                    .startsWith("TenantUser:")));
  }

  @Test
  public void testClusterResource() throws Throwable {
    startTestHarness(brokerProps(true));

    AdminClient adminClient = testHarness.createAdminClient(user1);
    try {
      adminClient.describeConfigs(Collections.singleton(new ConfigResource(Type.BROKER, "0")))
          .all().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ClusterAuthorizationException) {
        // expected
      } else {
        throw e;
      }
    }

    // for debugging
    List<ConfluentAuthorizationEvent> user1s = MockAuditLogProvider.instance.auditLog.stream()
        .filter(e -> e.requestContext().principal().toString().equals("User:1")
        ).collect(toList());

    // make sure we have an appropriate describe denial event
    List<ConfluentAuthorizationEvent> describes = MockAuditLogProvider.instance.auditLog.stream()
        .filter(e -> e.requestContext().principal().toString().equals("User:1") &&
            e.action().resourceName().equals(CLUSTER_NAME) &&
            e.action().operation().name().equals("DescribeConfigs") &&
            e.authorizeResult() == AuthorizeResult.DENIED
        ).collect(toList());
    assertEquals(1, describes.size());
  }

  private Properties brokerProps(boolean auditLoggerEnable) {
    Properties props = new Properties();
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
        MultiTenantAuthorizer.class.getName());
    props.put(MultiTenantAuthorizer.MAX_ACLS_PER_TENANT_PROP, "100");
    if (auditLoggerEnable) {
      // this is disabled by default, so we don't need to explicitly set it to false
      props.put(MultiTenantAuditLogConfig.MULTI_TENANT_AUDIT_LOGGER_ENABLE_CONFIG, "true");
    }
    return props;
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

}