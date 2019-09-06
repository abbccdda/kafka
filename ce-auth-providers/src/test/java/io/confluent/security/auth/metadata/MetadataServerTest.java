// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.metadata.MockMetadataServer.ServerState;
import io.confluent.security.auth.provider.rbac.RbacProvider;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

public class MetadataServerTest {

  private final Scope clusterA = new Scope.Builder("testOrg").withKafkaCluster("clusterA").build();
  private final ResourcePattern clusterResource = new ResourcePattern(new ResourceType("Cluster"), "kafka-cluster", PatternType.LITERAL);
  private EmbeddedAuthorizer authorizer;
  private RbacProvider metadataRbacProvider;
  private MockMetadataServer metadataServer;
  private ResourcePattern topic = new ResourcePattern("Topic", "topicA", PatternType.LITERAL);

  @After
  public void tearDown() {
    if (authorizer != null)
      authorizer.close();
  }

  @Test
  public void testMetadataServer() throws Exception {
    createEmbeddedAuthorizer(Collections.emptyMap());
    waitForMetadataServer();
    verifyMetadataServer(Scope.ROOT_SCOPE, null);
  }

  @Test
  public void testNoRbacProviderOnBrokerWithoutRbacAccessControl() throws Exception {
    createEmbeddedAuthorizer(Collections.singletonMap(
        ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "SUPER_USERS"));
    assertFalse(authorizer.metadataProvider() instanceof RbacProvider);
  }

  private void verifyMetadataServer(Scope cacheScope, Scope invalidScope) {
    assertEquals(ServerState.STARTED, metadataServer.serverState);
    assertNotNull(metadataServer.authStore);
    assertNotNull(metadataServer.embeddedAuthorizer);
    assertTrue(metadataServer.embeddedAuthorizer instanceof EmbeddedAuthorizer);
    EmbeddedAuthorizer authorizer = (EmbeddedAuthorizer) metadataServer.embeddedAuthorizer;
    metadataRbacProvider = (RbacProvider) authorizer.accessRuleProvider("MOCK_RBAC");
    assertNotNull(metadataRbacProvider);
    DefaultAuthCache metadataAuthCache = (DefaultAuthCache) metadataRbacProvider.authStore().authCache();
    assertSame(metadataServer.authCache, metadataAuthCache);
    assertEquals(cacheScope, metadataAuthCache.rootScope());
    assertEquals("clusterA", metadataServer.metadataClusterId);

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    RbacTestUtils.updateRoleBinding(metadataAuthCache, alice, "ClusterAdmin", clusterA, Collections.emptySet());
    verifyRules(accessRules(alice, groups, clusterResource), "Create", "Alter", "Describe", "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic), "Create", "Delete", "Alter", "AlterConfigs", "Describe", "DescribeConfigs");

    Action alterConfigs = new Action(clusterA, new ResourceType("Cluster"), "kafka-cluster", new Operation("AlterConfigs"));
    assertEquals(Collections.singletonList(AuthorizeResult.ALLOWED),
        metadataServer.embeddedAuthorizer.authorize(alice, "localhost", Collections.singletonList(alterConfigs)));

    Action readTopic = new Action(clusterA, new ResourceType("Topic"), "testtopic", new Operation("Read"));
    assertEquals(Collections.singletonList(AuthorizeResult.DENIED),
        metadataServer.embeddedAuthorizer.authorize(alice, "localhost", Collections.singletonList(readTopic)));

    if (invalidScope != null) {
      Action describeAnotherScope = new Action(invalidScope, new ResourceType("Cluster"), "kafka-cluster", new Operation("AlterConfigs"));
      assertEquals(Collections.singletonList(AuthorizeResult.DENIED),
          metadataServer.embeddedAuthorizer.authorize(alice, "localhost", Collections.singletonList(describeAnotherScope)));
    }
  }

  @Test
  public void testMetadataServerConfigs() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("confluent.metadata.server.custom.config", "some.value");
    configs.put("confluent.metadata.server.ssl.truststore.location", "trust.jks");
    configs.put("ssl.keystore.location", "key.jks");
    configs.put("listeners", "PLAINTEXT://0.0.0.0:9092");
    configs.put("advertised.listeners", "PLAINTEXT://localhost:9092");
    configs.put("confluent.metadata.server.listeners", "http://0.0.0.0:8090");
    configs.put("confluent.metadata.server.advertised.listeners", "http://localhost:8090");
    createEmbeddedAuthorizer(configs);
    waitForMetadataServer();
    assertEquals("some.value", metadataServer.configs.get("custom.config"));
    assertEquals("trust.jks", metadataServer.configs.get("ssl.truststore.location"));
    assertNull(metadataServer.configs.get("ssl.keystore.location"));
    assertEquals("http://0.0.0.0:8090", metadataServer.configs.get("listeners"));
    assertEquals("http://localhost:8090", metadataServer.configs.get("advertised.listeners"));
  }

  private void createEmbeddedAuthorizer(Map<String, Object> configOverrides) throws Exception {
    authorizer = new EmbeddedAuthorizer();
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "MOCK_RBAC");
    configs.put(MetadataServiceConfig.METADATA_SERVER_LISTENERS_PROP, "http://localhost:8090");
    configs.put("listeners", "PLAINTEXT://localhost:9092");
    configs.put("super.users", "User:admin;Group:adminGroup");
    configs.putAll(configOverrides);
    authorizer.configure(configs);
    authorizer.configureServerInfo(KafkaTestUtils.serverInfo("clusterA", SecurityProtocol.SSL));
    authorizer.start(configs).get();
  }

  private void waitForMetadataServer() throws Exception {
    try {
      TestUtils.waitForCondition(() -> metadataServer(authorizer) != null,
          "Metadata server not created");
      this.metadataServer = metadataServer(authorizer);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private MockMetadataServer metadataServer(EmbeddedAuthorizer authorizer) {
    assertTrue(authorizer.metadataProvider() instanceof  RbacProvider);
    MetadataServer metadataServer = ((RbacProvider) authorizer.metadataProvider()).metadataServer();
    if (metadataServer != null) {
      assertTrue(metadataServer instanceof MockMetadataServer);
      return (MockMetadataServer) metadataServer;
    } else
      return null;
  }

  private Set<AccessRule> accessRules(KafkaPrincipal userPrincipal,
                                      Set<KafkaPrincipal> groupPrincipals,
                                      ResourcePattern resource) {
    return metadataRbacProvider.accessRules(userPrincipal, groupPrincipals, clusterA, resource);
  }

  private void verifyRules(Set<AccessRule> rules, String... expectedOps) {
    Set<String> actualOps = rules.stream().map(r -> r.operation().name()).collect(Collectors.toSet());
    assertEquals(Utils.mkSet(expectedOps), actualOps);
  }

}

