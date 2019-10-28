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
import kafka.admin.AclCommand;
import kafka.network.RequestChannel.Session;
import kafka.security.auth.All$;
import kafka.security.auth.Alter$;
import kafka.security.auth.AlterConfigs$;
import kafka.security.auth.Cluster$;
import kafka.security.auth.ClusterAction$;
import kafka.security.auth.Create$;
import kafka.security.auth.Delete$;
import kafka.security.auth.Describe$;
import kafka.security.auth.DescribeConfigs$;
import kafka.security.auth.Group$;
import kafka.security.auth.IdempotentWrite$;
import kafka.security.auth.Operation;
import kafka.security.auth.Operation$;
import kafka.security.auth.Read$;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType;
import kafka.security.auth.Topic$;
import kafka.security.auth.Write$;
import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AuthorizerUtils;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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

  private static final Collection<Operation> TOPIC_OPS = Arrays.asList(
      Read$.MODULE$, Write$.MODULE$, Delete$.MODULE$, Describe$.MODULE$,
      AlterConfigs$.MODULE$, DescribeConfigs$.MODULE$);
  private static final Collection<Operation> CONSUMER_GROUP_OPS = Arrays.asList(
      Read$.MODULE$, Delete$.MODULE$, Describe$.MODULE$);
  private static final Collection<Operation> CLUSTER_OPS = Arrays.asList(
      Create$.MODULE$, ClusterAction$.MODULE$, DescribeConfigs$.MODULE$, AlterConfigs$.MODULE$,
      IdempotentWrite$.MODULE$, Alter$.MODULE$, Describe$.MODULE$);
  private static final Resource CLUSTER_RESOURCE =
      new Resource(Cluster$.MODULE$, Resource.ClusterResourceName(), PatternType.LITERAL);

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

    Resource topicResource = randomResource(Topic$.MODULE$);
    verifyResourceAcls(topicResource, TOPIC_OPS,
        (principal, op) -> addTopicAcl(principal, topicResource, op));
    Resource consumerGroup = randomResource(Group$.MODULE$);
    verifyResourceAcls(consumerGroup, CONSUMER_GROUP_OPS,
        (principal, op) -> addConsumerGroupAcl(principal, consumerGroup, op));
    verifyResourceAcls(CLUSTER_RESOURCE, CLUSTER_OPS, this::addClusterAcl);

    verifyAllOperationsAcl(Topic$.MODULE$, TOPIC_OPS,
        (principal, resource) -> addTopicAcl(principal, resource, All$.MODULE$));
    verifyAllOperationsAcl(Group$.MODULE$, CONSUMER_GROUP_OPS,
        (principal, resource) -> addConsumerGroupAcl(principal, resource, All$.MODULE$));
  }

  @Test
  public void testGroupChanges() throws Exception {
    miniKdcWithLdapService.createGroup("adminGroup", "adminUser", "kafkaUser");
    miniKdcWithLdapService.createGroup("guestGroup", "guest");
    configureLdapAuthorizer();

    KafkaPrincipal adminGroupPrincipal = new KafkaPrincipal("Group", "adminGroup");
    Resource topicResource = randomResource(Topic$.MODULE$);
    addTopicAcl(adminGroupPrincipal, topicResource, All$.MODULE$);
    Resource consumerGroup = randomResource(Group$.MODULE$);
    addConsumerGroupAcl(adminGroupPrincipal, consumerGroup, All$.MODULE$);
    Resource clusterResource =
        new Resource(Cluster$.MODULE$, Resource.ClusterResourceName(), PatternType.LITERAL);
    addClusterAcl(adminGroupPrincipal, All$.MODULE$);

    TOPIC_OPS.forEach(op -> verifyAuthorization("anotherUser", topicResource, op, false));
    CONSUMER_GROUP_OPS.forEach(op -> verifyAuthorization("anotherUser", consumerGroup, op, false));
    CLUSTER_OPS.forEach(op -> verifyAuthorization("anotherUser", clusterResource, op, false));

    miniKdcWithLdapService.addUserToGroup("adminGroup", "anotherUser");
    LdapTestUtils.waitForUserGroups(ldapAuthorizer, "anotherUser", "adminGroup");

    TOPIC_OPS.forEach(op -> verifyAuthorization("anotherUser", topicResource, op, true));
    CONSUMER_GROUP_OPS.forEach(op -> verifyAuthorization("anotherUser", consumerGroup, op, true));
    CLUSTER_OPS.forEach(op -> verifyAuthorization("anotherUser", clusterResource, op, true));

    verifyAuthorization("anotherUser", topicResource, Write$.MODULE$, true);
    deleteTopicAcl(adminGroupPrincipal, topicResource, All$.MODULE$);
    verifyAuthorization("anotherUser", topicResource, Write$.MODULE$, false);
  }

  @Test
  public void testLdapFailure() throws Exception {
    miniKdcWithLdapService.createGroup("adminGroup", "adminUser", "kafkaUser");
    miniKdcWithLdapService.createGroup("guestGroup", "guest");
    authorizerConfig.put(LdapConfig.RETRY_TIMEOUT_MS_PROP, "1000");
    configureLdapAuthorizer();

    KafkaPrincipal adminGroupPrincipal = new KafkaPrincipal("Group", "adminGroup");
    Resource topicResource = randomResource(Topic$.MODULE$);
    addTopicAcl(adminGroupPrincipal, topicResource, All$.MODULE$);

    TOPIC_OPS.forEach(op -> verifyAuthorization("adminUser", topicResource, op, true));

    miniKdcWithLdapService.shutdown();
    TestUtils.waitForCondition(() -> {
      AuthorizableRequestContext requestContext = AuthorizerUtils.sessionToRequestContext(session("adminUser"));
      Action action = new Action(AclOperation.READ, topicResource.toPattern(), 1, true, true);
      return ldapAuthorizer.authorize(requestContext, Collections.singletonList(action)).get(0) == AuthorizationResult.DENIED;
    }, "LDAP failure not detected");
  }

  private void verifyResourceAcls(Resource resource, Collection<Operation> ops,
      BiFunction<KafkaPrincipal, Operation, Void> addAcl) {
    for (Operation op : ops) {
      addAcl.apply(new KafkaPrincipal("Group", "adminGroup"), op);
      verifyAuthorization("adminUser", resource, op, true);
      verifyAuthorization("guestUser", resource, op, false);
      String someUser = "someUser" + op;
      verifyAuthorization(someUser, resource, op, false);
      addAcl.apply(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, someUser), op);
      verifyAuthorization(someUser, resource, op, true);
    }
  }

  private void verifyAllOperationsAcl(ResourceType resourceType, Collection<Operation> ops,
      BiFunction<KafkaPrincipal, Resource, Void> addAcl) {
    Resource resource = randomResource(resourceType);
    addAcl.apply(new KafkaPrincipal("Group", "adminGroup"), resource);
    addAcl.apply(new KafkaPrincipal("User", "someUser"), resource);
    for (Operation op : ops) {
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

    Resource topicResource = randomResource(Topic$.MODULE$);
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
    Resource topicResource = randomResource(Topic$.MODULE$);
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

  private Resource randomResource(ResourceType resourceType) {
    return new Resource(resourceType, UUID.randomUUID().toString(), PatternType.LITERAL);
  }

  private void verifyAuthorization(String user, Resource resource, boolean allowed) {
    scala.collection.Iterator<Operation> opsIter = Operation$.MODULE$.values().iterator();
    while (opsIter.hasNext()) {
      Operation op = opsIter.next();
      verifyAuthorization(user, resource, op, allowed);
    }
  }

  private void verifyAuthorization(String user, Resource resource, Operation op, boolean allowed) {
    List<AuthorizationResult> expectedResults = Collections.singletonList(allowed ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED);
    AuthorizableRequestContext requestContext = AuthorizerUtils.sessionToRequestContext(session(user));
    Action action = new Action(op.toJava(), resource.toPattern(), 1, true, true);
    assertEquals("Incorrect authorization result for op " + op,
        expectedResults, ldapAuthorizer.authorize(requestContext, Collections.singletonList(action)));
  }

  private Void addTopicAcl(KafkaPrincipal principal, Resource topic, Operation op) {
    AclCommand.main(SecurityTestUtils.addTopicAclArgs(kafkaCluster.zkConnect(),
        principal, topic.name(), op, PatternType.LITERAL));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, topic, op, false);
    return null;
  }

  private Void addConsumerGroupAcl(KafkaPrincipal principal, Resource group, Operation op) {
    AclCommand.main(SecurityTestUtils.addConsumerGroupAclArgs(kafkaCluster.zkConnect(),
        principal, group.name(), op, PatternType.LITERAL));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, group, op, false);
    return null;
  }

  private Void addClusterAcl(KafkaPrincipal principal, Operation op) {
    AclCommand.main(SecurityTestUtils.clusterAclArgs(kafkaCluster.zkConnect(),
        principal, op.name()));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, CLUSTER_RESOURCE, op, false);
    return null;
  }

  private void deleteTopicAcl(KafkaPrincipal principal, Resource topic, Operation op) {
    AclCommand.main(SecurityTestUtils.deleteTopicAclArgs(kafkaCluster.zkConnect(),
        principal, topic.name(), op.name()));
    SecurityTestUtils.waitForAclUpdate(ldapAuthorizer, principal, topic, op, true);
  }
}

