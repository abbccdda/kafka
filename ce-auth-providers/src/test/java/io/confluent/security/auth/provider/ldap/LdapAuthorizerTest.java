// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.license.InvalidLicenseException;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.test.utils.LdapTestUtils;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import kafka.admin.AclCommand;
import kafka.network.RequestChannel.Session;
import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AuthorizerUtils;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class LdapAuthorizerTest {

  private static final Collection<AclOperation> TOPIC_OPS = Arrays.asList(
      AclOperation.READ, AclOperation.WRITE, AclOperation.DELETE, AclOperation.DESCRIBE,
      AclOperation.ALTER_CONFIGS, AclOperation.DESCRIBE_CONFIGS);
  private static final Collection<AclOperation> CONSUMER_GROUP_OPS = Arrays.asList(
      AclOperation.READ, AclOperation.DELETE, AclOperation.DESCRIBE);
  private static final Collection<AclOperation> CLUSTER_OPS = Arrays.asList(
      AclOperation.CREATE, AclOperation.CLUSTER_ACTION, AclOperation.DESCRIBE_CONFIGS, AclOperation.ALTER_CONFIGS,
      AclOperation.IDEMPOTENT_WRITE, AclOperation.ALTER, AclOperation.DESCRIBE);
  private static final ResourcePattern CLUSTER_RESOURCE =
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);

  private EmbeddedKafkaCluster kafkaCluster;
  private MiniKdcWithLdapService miniKdcWithLdapService;
  private LdapAuthorizer ldapAuthorizer;
  private Map<String, Object> authorizerConfig;

  @Before
  public void setUp() throws Exception {
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);
    ldapAuthorizer = new LdapAuthorizer();
    authorizerConfig = new HashMap<>();
    authorizerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), kafkaCluster.zkConnect());
    authorizerConfig.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 10));
  }

  @After
  public void tearDown() throws IOException {
    if (ldapAuthorizer != null) {
      ldapAuthorizer.close();
    }
    if (miniKdcWithLdapService != null) {
      miniKdcWithLdapService.shutdown();
    }
    if (kafkaCluster != null) {
      kafkaCluster.shutdown();
    }
  }

  @Test
  public void testAcls() throws Exception {
    miniKdcWithLdapService.createGroup("adminGroup", "adminUser", "kafkaUser");
    miniKdcWithLdapService.createGroup("guestGroup", "guest");
    configureLdapAuthorizer();

    ResourcePattern topicResource = randomResource(ResourceType.TOPIC);
    verifyResourceAcls(topicResource, TOPIC_OPS,
        (principal, op) -> addTopicAcl(principal, topicResource, op));
    ResourcePattern consumerGroup = randomResource(ResourceType.GROUP);
    verifyResourceAcls(consumerGroup, CONSUMER_GROUP_OPS,
        (principal, op) -> addConsumerGroupAcl(principal, consumerGroup, op));
    verifyResourceAcls(CLUSTER_RESOURCE, CLUSTER_OPS, this::addClusterAcl);

    verifyAllOperationsAcl(ResourceType.TOPIC, TOPIC_OPS,
        (principal, resource) -> addTopicAcl(principal, resource, AclOperation.ALL));
    verifyAllOperationsAcl(ResourceType.GROUP, CONSUMER_GROUP_OPS,
        (principal, resource) -> addConsumerGroupAcl(principal, resource, AclOperation.ALL));
  }

  @Test
  public void testGroupChanges() throws Exception {
    miniKdcWithLdapService.createGroup("adminGroup", "adminUser", "kafkaUser");
    miniKdcWithLdapService.createGroup("guestGroup", "guest");
    configureLdapAuthorizer();

    KafkaPrincipal adminGroupPrincipal = new KafkaPrincipal("Group", "adminGroup");
    ResourcePattern topicResource = randomResource(ResourceType.TOPIC);
    addTopicAcl(adminGroupPrincipal, topicResource, AclOperation.ALL);
    ResourcePattern consumerGroup = randomResource(ResourceType.GROUP);
    addConsumerGroupAcl(adminGroupPrincipal, consumerGroup, AclOperation.ALL);
    ResourcePattern clusterResource =
        new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);
    addClusterAcl(adminGroupPrincipal, AclOperation.ALL);

    TOPIC_OPS.forEach(op -> verifyAuthorization("anotherUser", topicResource, op, false));
    CONSUMER_GROUP_OPS.forEach(op -> verifyAuthorization("anotherUser", consumerGroup, op, false));
    CLUSTER_OPS.forEach(op -> verifyAuthorization("anotherUser", clusterResource, op, false));

    miniKdcWithLdapService.addUserToGroup("adminGroup", "anotherUser");
    LdapTestUtils.waitForUserGroups(ldapAuthorizer, "anotherUser", "adminGroup");

    TOPIC_OPS.forEach(op -> verifyAuthorization("anotherUser", topicResource, op, true));
    CONSUMER_GROUP_OPS.forEach(op -> verifyAuthorization("anotherUser", consumerGroup, op, true));
    CLUSTER_OPS.forEach(op -> verifyAuthorization("anotherUser", clusterResource, op, true));

    verifyAuthorization("anotherUser", topicResource, AclOperation.WRITE, true);
    deleteTopicAcl(adminGroupPrincipal, topicResource, AclOperation.ALL);
    verifyAuthorization("anotherUser", topicResource, AclOperation.WRITE, false);
  }

  @Test
  public void testLdapFailure() throws Exception {
    miniKdcWithLdapService.createGroup("adminGroup", "adminUser", "kafkaUser");
    miniKdcWithLdapService.createGroup("guestGroup", "guest");
    authorizerConfig.put(LdapConfig.RETRY_TIMEOUT_MS_PROP, "1000");
    configureLdapAuthorizer();

    KafkaPrincipal adminGroupPrincipal = new KafkaPrincipal("Group", "adminGroup");
    ResourcePattern topicResource = randomResource(ResourceType.TOPIC);
    addTopicAcl(adminGroupPrincipal, topicResource, AclOperation.ALL);

    TOPIC_OPS.forEach(op -> verifyAuthorization("adminUser", topicResource, op, true));

    miniKdcWithLdapService.shutdown();
    TestUtils.waitForCondition(() -> {
      AuthorizableRequestContext requestContext = AuthorizerUtils.sessionToRequestContext(session("adminUser"));
      Action action = new Action(AclOperation.READ, topicResource, 1, true, true);
      return ldapAuthorizer.authorize(requestContext, Collections.singletonList(action)).get(0) == AuthorizationResult.DENIED;
    }, "LDAP failure not detected");
  }

  private void verifyResourceAcls(ResourcePattern resource, Collection<AclOperation> ops,
      BiFunction<KafkaPrincipal, AclOperation, Void> addAcl) {
    for (AclOperation op : ops) {
      addAcl.apply(new KafkaPrincipal("Group", "adminGroup"), op);
      verifyAuthorization("adminUser", resource, op, true);
      verifyAuthorization("guestUser", resource, op, false);
      String someUser = "someUser" + op;
      verifyAuthorization(someUser, resource, op, false);
      addAcl.apply(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, someUser), op);
      verifyAuthorization(someUser, resource, op, true);
    }
  }

  private void verifyAllOperationsAcl(ResourceType resourceType, Collection<AclOperation> ops,
      BiFunction<KafkaPrincipal, ResourcePattern, Void> addAcl) {
    ResourcePattern resource = randomResource(resourceType);
    addAcl.apply(new KafkaPrincipal("Group", "adminGroup"), resource);
    addAcl.apply(new KafkaPrincipal("User", "someUser"), resource);
    for (AclOperation op : ops) {
      verifyAuthorization("adminUser", resource, op, true);
      verifyAuthorization("someUser", resource, op, true);
      verifyAuthorization("guestUser", resource, op, false);
    }
  }

  @Test
  public void testSuperGroups() throws Exception {
    verifySuperUser(false);
  }

  @Test
  public void testAllowIfNoAcl() throws Exception {
    verifySuperUser(true);
  }

  private void verifySuperUser(boolean allowIfNoAcl) throws Exception {
    miniKdcWithLdapService.createGroup("adminGroup", "adminUser");
    miniKdcWithLdapService.createGroup("guestGroup", "guest");

    authorizerConfig.put(AclAuthorizer.SuperUsersProp(), "Group:adminGroup");
    authorizerConfig.put(AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp(),
        String.valueOf(allowIfNoAcl));
    configureLdapAuthorizer();

    ResourcePattern topicResource = randomResource(ResourceType.TOPIC);
    verifyAuthorization("adminUser", topicResource, true);
    verifyAuthorization("guestUser", topicResource, allowIfNoAcl);
    verifyAuthorization("someUser", topicResource, allowIfNoAcl);
  }


  private void configureLdapAuthorizer() throws Exception {
    ldapAuthorizer.configure(authorizerConfig);
    ldapAuthorizer.start(KafkaTestUtils.serverInfo("clusterA", SecurityProtocol.SSL)).values()
        .forEach(stage -> stage.toCompletableFuture().join());
  }

  private void verifyAuthorizer() throws Exception {
    miniKdcWithLdapService.createGroup("adminGroup", "adminUser", "kafkaUser");
    miniKdcWithLdapService.createGroup("guestGroup", "guest");
    ResourcePattern topicResource = randomResource(ResourceType.TOPIC);
    verifyResourceAcls(topicResource, TOPIC_OPS,
        (principal, op) -> addTopicAcl(principal, topicResource, op));
  }

  private void verifyAuthorizerLicenseFailure() {
    ldapAuthorizer.configure(authorizerConfig);
    try {
      ldapAuthorizer.start(KafkaTestUtils.serverInfo("clusterA", SecurityProtocol.SSL)).values()
          .forEach(stage -> stage.toCompletableFuture().join());
    } catch (Exception e) {
      Throwable cause = e.getCause();
      while (cause != null && !(cause instanceof InvalidLicenseException)) {
        cause = cause.getCause();
      }
      assertNotNull("Unexpected exception: " + e, cause);
    }
  }

  private Session session(String user) {
    try {
      return new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user),
          InetAddress.getByName("localhost"));
    } catch (Exception e) {
      throw new RuntimeException("Session could not be created for " + user, e);
    }
  }

  private ResourcePattern randomResource(ResourceType resourceType) {
    return new ResourcePattern(resourceType, UUID.randomUUID().toString(), PatternType.LITERAL);
  }

  private void verifyAuthorization(String user, ResourcePattern resource, boolean allowed) {
    Stream.of(AclOperation.values()).forEach(kafkaOperation -> verifyAuthorization(user, resource, kafkaOperation, allowed));
  }

  private void verifyAuthorization(String user, ResourcePattern resource, AclOperation op, boolean allowed) {
    List<AuthorizationResult> expectedResults = Collections.singletonList(allowed ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED);
    AuthorizableRequestContext requestContext = AuthorizerUtils.sessionToRequestContext(session(user));
    Action action = new Action(op, resource, 1, true, true);
    assertEquals("Incorrect authorization result for op " + op,
        expectedResults, ldapAuthorizer.authorize(requestContext, Collections.singletonList(action)));
  }

  private Void addTopicAcl(KafkaPrincipal principal, ResourcePattern topic, AclOperation op) {
    AclCommand.main(SecurityTestUtils.addTopicAclArgs(kafkaCluster.zkConnect(),
        principal, topic.name(), op, PatternType.LITERAL));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, topic, op, false);
    return null;
  }

  private Void addConsumerGroupAcl(KafkaPrincipal principal, ResourcePattern group, AclOperation op) {
    AclCommand.main(SecurityTestUtils.addConsumerGroupAclArgs(kafkaCluster.zkConnect(),
        principal, group.name(), op, PatternType.LITERAL));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, group, op, false);
    return null;
  }

  private Void addClusterAcl(KafkaPrincipal principal, AclOperation op) {
    AclCommand.main(SecurityTestUtils.clusterAclArgs(kafkaCluster.zkConnect(),
        principal, SecurityUtils.operationName(op)));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, CLUSTER_RESOURCE, op, false);
    return null;
  }

  private void deleteTopicAcl(KafkaPrincipal principal, ResourcePattern topic, AclOperation op) {
    AclCommand.main(SecurityTestUtils.deleteTopicAclArgs(kafkaCluster.zkConnect(),
        principal, topic.name(), SecurityUtils.operationName(op)));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, topic, op, true);
  }
}

