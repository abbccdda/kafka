// (Copyright) [2019 - 2019] Confluent, Inc.
package io.confluent.kafka.security.authorizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.AclAccessRule;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.AuthorizeRule;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.server.audit.AuditLogProvider;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.internals.ConfluentAuthorizerServerInfo;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MockConfluentServerAuthorizerTest {

  private ConfluentServerAuthorizer authorizer;
  private ConfluentAuthorizerServerInfo serverInfo;
  private Endpoint controlPlaneEndpoint;
  private Endpoint interBrokerEndpoint;
  private Endpoint externalEndpoint;
  private ExecutorService executorService;
  private volatile Map<Endpoint, ? extends CompletionStage<Void>> startFutures;

  @Before
  public void setUp() throws Exception {
    MockAclProvider.reset();
    MockAuditLogProvider.reset();
    authorizer = new ConfluentServerAuthorizer();
    executorService = Executors.newSingleThreadExecutor();

    Map<String, Object> configs = new HashMap<>();
    configs.put(KafkaConfig$.MODULE$.BrokerIdProp(), 1);
    configs.put(KafkaConfig$.MODULE$.ZkConnectProp(), "localhost:2181");
    configs.put(KafkaConfig$.MODULE$.ControlPlaneListenerNameProp(), "control");
    configs.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "internal");
    configs.put(KafkaConfig$.MODULE$.ListenersProp(), "control://:9090,internal://:9091,external://:9092");
    configs.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(), "control:SSL,internal:PLAINTEXT,external:SASL_SSL");
    configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "MOCK_ACL");
    authorizer.configure(configs);

    controlPlaneEndpoint = new Endpoint("control", SecurityProtocol.SSL, "localhost", 9090);
    interBrokerEndpoint = new Endpoint("internal", SecurityProtocol.PLAINTEXT, "localhost", 9091);
    externalEndpoint = new Endpoint("external", SecurityProtocol.SASL_SSL, "localhost", 9092);

    serverInfo = new ConfluentAuthorizerServerInfo() {
      @Override
      public ClusterResource clusterResource() {
        return new ClusterResource("clusterA");
      }

      @Override
      public int brokerId() {
        return 1;
      }

      @Override
      public Collection<Endpoint> endpoints() {
        return Arrays.asList(controlPlaneEndpoint, interBrokerEndpoint, externalEndpoint);
      }

      @Override
      public Endpoint interBrokerEndpoint() {
        return interBrokerEndpoint;
      }

      @Override
      public AuditLogProvider auditLogProvider() {
        return new MockAuditLogProvider();
      }

      @Override
      public Metrics metrics() {
        return new Metrics();
      };
    };
  }

  @After
  public void tearDown() throws Exception {
    executorService.shutdownNow();
    authorizer.close();
    MockAclProvider.reset();
    MockAuditLogProvider.reset();
  }

  @Test
  public void testStartupSequenceInMdsCluster() throws Exception {
    MockAclProvider.usesMetadataFromThisKafkaCluster = true;
    startAuthorizer();
    TestUtils.waitForCondition(() -> this.startFutures != null, "Authorizer start not complete");
    assertTrue(startFutures.get(controlPlaneEndpoint).toCompletableFuture().isDone());
    assertTrue(startFutures.get(interBrokerEndpoint).toCompletableFuture().isDone());
    assertFalse(startFutures.get(externalEndpoint).toCompletableFuture().isDone());

    MockAclProvider.startFuture.complete(null);
    startFutures.get(externalEndpoint).toCompletableFuture().get(10, TimeUnit.SECONDS);
    MockAuditLogProvider.instance.ensureStarted();
  }

  @Test
  public void testStartupSequenceInNonMdsCluster() throws Exception {
    MockAclProvider.usesMetadataFromThisKafkaCluster = false;
    startAuthorizer();
    assertNull(startFutures);
    MockAclProvider.startFuture.complete(null);
    TestUtils.waitForCondition(() -> this.startFutures != null, "Authorizer start not complete");
    assertTrue(startFutures.values().stream().allMatch(future -> future.toCompletableFuture().isDone()));
    MockAuditLogProvider.instance.ensureStarted();
  }

  @Test
  public void testAuditLogEntries() throws Exception {
    MockAclProvider.startFuture.complete(null);
    authorizer.start(serverInfo).values().forEach(future -> future.toCompletableFuture().join());

    AuthorizableRequestContext requestContext = new org.apache.kafka.common.requests.RequestContext(
        null, "", InetAddress.getLocalHost(), KafkaPrincipal.ANONYMOUS,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY);
    org.apache.kafka.server.authorizer.Action allowedWithLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "allowedWithLog", PatternType.LITERAL),
        1, true, false);
    org.apache.kafka.server.authorizer.Action allowedNoLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "allowedNoLog", PatternType.LITERAL),
        1, false, true);
    org.apache.kafka.server.authorizer.Action deniedWithLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "deniedWithLog", PatternType.LITERAL),
        1, false, true);
    org.apache.kafka.server.authorizer.Action deniedNoLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "deniedNoLog", PatternType.LITERAL),
        1, true, false);

    assertEquals(AuthorizationResult.ALLOWED, authorizer.authorize(requestContext, Collections.singletonList(allowedWithLog)).get(0));

    MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
    assertEquals(1, auditLogProvider.authorizationLog.size());
    assertEquals("allowedWithLog", auditLogProvider.lastAuthorizationEntry().action().resourcePattern().name());
    assertEquals(AuthorizeResult.ALLOWED, auditLogProvider.lastAuthorizationEntry().authorizeResult());
    assertEquals(KafkaPrincipal.ANONYMOUS, auditLogProvider.lastAuthorizationEntry().requestContext().principal());
    assertEquals(PolicyType.ALLOW_ACL, auditLogProvider.lastAuthorizationEntry().authorizePolicy().policyType());
    auditLogProvider.authorizationLog.clear();

    assertEquals(AuthorizationResult.ALLOWED, authorizer.authorize(requestContext, Collections.singletonList(allowedNoLog)).get(0));
    assertTrue(auditLogProvider.authorizationLog.isEmpty());

    assertEquals(AuthorizationResult.DENIED, authorizer.authorize(requestContext, Collections.singletonList(deniedWithLog)).get(0));
    assertEquals(1, auditLogProvider.authorizationLog.size());
    assertEquals("deniedWithLog", auditLogProvider.lastAuthorizationEntry().action().resourcePattern().name());
    assertEquals(AuthorizeResult.DENIED, auditLogProvider.lastAuthorizationEntry().authorizeResult());
    assertEquals(PolicyType.DENY_ON_NO_RULE, auditLogProvider.lastAuthorizationEntry().authorizePolicy().policyType());
    auditLogProvider.authorizationLog.clear();

    assertEquals(AuthorizationResult.DENIED, authorizer.authorize(requestContext, Collections.singletonList(deniedNoLog)).get(0));
  }

  @Test
  public void testAuditLogException() throws Exception {
    MockAclProvider.startFuture.complete(null);
    authorizer.start(serverInfo).values().forEach(future -> future.toCompletableFuture().join());
    MockAuditLogProvider.instance.setFail(true);

    AuthorizableRequestContext requestContext = new org.apache.kafka.common.requests.RequestContext(
        null, "", InetAddress.getLocalHost(), KafkaPrincipal.ANONYMOUS,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT,
        ClientInformation.EMPTY);
    org.apache.kafka.server.authorizer.Action allowedWithLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "allowedWithLog",
            PatternType.LITERAL),
        1, true, false);
    org.apache.kafka.server.authorizer.Action allowedNoLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "allowedNoLog",
            PatternType.LITERAL),
        1, false, true);
    org.apache.kafka.server.authorizer.Action deniedWithLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "deniedWithLog",
            PatternType.LITERAL),
        1, false, true);
    org.apache.kafka.server.authorizer.Action deniedNoLog = new org.apache.kafka.server.authorizer.Action(
        AclOperation.DESCRIBE,
        new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, "deniedNoLog",
            PatternType.LITERAL),
        1, true, false);

    // Even though the audit logger threw an exception, the authorization succeeded
    assertEquals(AuthorizationResult.ALLOWED,
        authorizer.authorize(requestContext, Collections.singletonList(allowedWithLog)).get(0));

    MockAuditLogProvider auditLogProvider = MockAuditLogProvider.instance;
    assertTrue(auditLogProvider.authorizationLog.isEmpty());
  }

  private void startAuthorizer() {
    executorService.submit(() -> {
      startFutures = authorizer.start(serverInfo);
    });
  }

  public static final class MockAclProvider implements AccessRuleProvider {

    static boolean usesMetadataFromThisKafkaCluster;
    static CompletableFuture<Void> startFuture;

    @Override
    public String providerName() {
      return "MOCK_ACL";
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public CompletionStage<Void> start(ConfluentAuthorizerServerInfo serverInfo, Map<String, ?> clientConfigs) {
      // Metrics reporter doesn't like broker.id in client configs
      assertTrue(!clientConfigs.containsKey(KafkaConfig$.MODULE$.BrokerIdProp()));
      return startFuture;
    }

    @Override
    public boolean usesMetadataFromThisKafkaCluster() {
      return usesMetadataFromThisKafkaCluster;
    }

    @Override
    public boolean isSuperUser(KafkaPrincipal principal, Scope scope) {
      return false;
    }

    @Override
    public AuthorizeRule findRule(KafkaPrincipal principal,
                                  Set<KafkaPrincipal> groupPrincipals,
                                  String host,
                                  Action action) {
      ResourcePattern resource = action.resourcePattern();
      AuthorizeRule authorizeRule = new AuthorizeRule();
      if (resource.name().startsWith("allowed")) {
        AccessRule rule = new AclAccessRule(resource, principal, PermissionType.ALLOW, "*",
            Operation.ALL, PolicyType.ALLOW_ACL,
            new AclBinding(ResourcePattern.to(resource),
                new AccessControlEntry(principal.getName(),
                    "*", AclOperation.ALL, AclPermissionType.ALLOW)));
        authorizeRule.addRuleIfNotExist(rule);
      }
      return authorizeRule;
    }

    @Override
    public boolean mayDeny() {
      return false;
    }

    @Override
    public void close() {
    }

    static void reset() {
      usesMetadataFromThisKafkaCluster = true;
      startFuture = new CompletableFuture<>();
    }
  }

}
