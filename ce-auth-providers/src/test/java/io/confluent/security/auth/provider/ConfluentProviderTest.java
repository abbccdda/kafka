// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.MockMetadataServer;
import io.confluent.security.auth.metadata.MockMetadataServer.ServerState;
import io.confluent.security.auth.provider.rbac.MockRbacProvider.MockAuthStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AclBindingKey;
import io.confluent.security.auth.store.data.AclBindingValue;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.acl.AclRule;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.test.utils.RbacTestUtils;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.ReplicaStatusOptions;
import org.apache.kafka.clients.admin.ReplicaStatusResult;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.http.MetadataServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfluentProviderTest {

  private final Scope clusterA = new Scope.Builder("testOrg").withKafkaCluster("clusterA").build();
  private final Scope clusterB = new Scope.Builder("testOrg").withKafkaCluster("clusterB").build();
  private final ResourcePattern clusterResource = new ResourcePattern(new ResourceType("Cluster"), "kafka-cluster", PatternType.LITERAL);
  private ConfluentProvider rbacProvider;
  private DefaultAuthCache authCache;
  private Optional<TestMdsAdminClient> aclClientOp;
  private ResourcePattern topic = new ResourcePattern("Topic", "topicA", PatternType.LITERAL);

  @Before
  public void setUp() throws Exception {
    initializeRbacProvider("clusterA", clusterA, Collections.emptyMap());
  }

  @After
  public void tearDown() {
    if (rbacProvider != null)
      rbacProvider.close();
  }

  @Test
  public void testSystemAdminAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "SystemAdmin", clusterA, null);
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));
    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    // Delete non-existing role
    deleteRoleBinding(alice, "SystemAdmin", clusterB);
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));

    deleteRoleBinding(alice, "SystemAdmin", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));
  }

  @Test
  public void testSystemAdminGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    updateRoleBinding(admin, "SystemAdmin", clusterA, Collections.emptySet());
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));
    assertFalse(rbacProvider.isSuperUser(admin, clusterA));
    verifyRules(alice, groups, clusterA, clusterResource, "All");
    verifyRules(alice, groups, clusterA, topic, "All");

    deleteRoleBinding(admin, "SystemAdmin", clusterA);
    assertFalse(rbacProvider.isSuperUser(alice, clusterA));
    assertFalse(rbacProvider.isSuperUser(admin, clusterA));

  }

  @Test
  public void testRbacAuthorizerSuperUsers() throws Exception {
    KafkaPrincipal admin = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin");
    Scope metadataCluster = Scope.kafkaClusterScope("metadataCluster");
    Scope otherCluster = Scope.kafkaClusterScope("anotherCluster");
    Map<String, Object> configs = new HashMap<>();
    configs.put("super.users", admin.toString());
    configs.put(MetadataServerConfig.METADATA_SERVER_LISTENERS_PROP, "http://127.0.0.1:8090");
    initializeRbacProvider("metadataCluster", Scope.ROOT_SCOPE, configs);
    EmbeddedAuthorizer authorizer = rbacProvider.createRbacAuthorizer();

    // Statically configured super users have access to security metadata in all clusters.
    // These users can also describe and alter access of any resource.
    // For the metadata service authorizer, these users are not granted access to any other resource.
    Operation alter = new Operation("Alter");
    Operation alterAccess = new Operation("AlterAccess");
    verifyAccess(authorizer, admin, metadataCluster, ConfluentProvider.SECURITY_METADATA, alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, admin, otherCluster, ConfluentProvider.SECURITY_METADATA, alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, admin, metadataCluster, topic.resourceType(), alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, admin, otherCluster, topic.resourceType(), alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, admin, metadataCluster, topic.resourceType(), alterAccess, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, admin, otherCluster, topic.resourceType(), alterAccess, AuthorizeResult.ALLOWED);

    // SystemAdmin role has access to all resources within the role binding scope
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    updateRoleBinding(alice, "SystemAdmin", metadataCluster, Collections.emptySet());
    verifyAccess(authorizer, alice, metadataCluster, ConfluentProvider.SECURITY_METADATA, alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, alice, otherCluster, ConfluentProvider.SECURITY_METADATA, alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, alice, metadataCluster, topic.resourceType(), alter, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, alice, otherCluster, topic.resourceType(), alter, AuthorizeResult.DENIED);
    verifyAccess(authorizer, alice, metadataCluster, topic.resourceType(), alterAccess, AuthorizeResult.ALLOWED);
    verifyAccess(authorizer, alice, otherCluster, topic.resourceType(), alterAccess, AuthorizeResult.DENIED);
  }

  @Test
  public void testClusterScopeAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "ClusterAdmin", clusterA, Collections.emptySet());
    verifyRules(alice, groups, clusterA, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, topic);

    updateRoleBinding(alice, "Operator", clusterA, Collections.emptySet());
    verifyRules(alice, groups, clusterA, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, topic, "DescribeConfigs", "AlterConfigs");
    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "ClusterAdmin", clusterA);
    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "Operator", clusterA);
    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);
  }

  @Test
  public void testClusterScopeGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    updateRoleBinding(admin, "ClusterAdmin", clusterA, Collections.emptySet());
    verifyRules(alice, groups, clusterA, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, topic);
    verifyRules(alice, Collections.emptySet(), clusterA, clusterResource);

    updateRoleBinding(admin, "Operator", clusterA, Collections.emptySet());
    verifyRules(alice, groups, clusterA, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, topic, "DescribeConfigs", "AlterConfigs");
    updateRoleBinding(admin, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");

    updateRoleBinding(alice, "Operator", clusterA, Collections.emptySet());
    verifyRules(alice, groups, clusterA, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(alice, "Operator", clusterA);
    verifyRules(alice, groups, clusterA, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(admin, "ClusterAdmin", clusterA);
    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");

    deleteRoleBinding(admin, "Operator", clusterA);
    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic);
  }

  @Test
  public void testLiteralResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", topic.name(), PatternType.LITERAL));
  }

  @Test
  public void testWildcardResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "*", PatternType.LITERAL));
  }

  @Test
  public void testPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "top", PatternType.PREFIXED));
  }

  @Test
  public void testSingleCharPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "t", PatternType.PREFIXED));
  }

  @Test
  public void testFullNamePrefixedResourceAccessRules() {
    verifyResourceAccessRules(new ResourcePattern("Topic", "topic", PatternType.PREFIXED));
  }

  /**
   * Verifies role-based authorization for empty resource name. Even though we disallow
   * role bindings with empty resource name, authorization of empty resource name is supported
   * since consumer group name may be empty.
   */
  @Test
  public void testEmptyResourceName() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    EmbeddedAuthorizer authorizer = rbacProvider.createRbacAuthorizer();

    Action action = new Action(clusterA, new ResourceType("Topic"), "", new Operation("Read"));
    List<Action> actions = Collections.singletonList(action);
    assertEquals(AuthorizeResult.DENIED, authorizer.authorize(alice, "", actions).get(0));

    ResourcePattern someResource = new ResourcePattern("Topic", "test", PatternType.LITERAL);
    updateRoleBinding(alice, "Reader", clusterA, Collections.singleton(someResource));
    assertEquals(AuthorizeResult.DENIED, authorizer.authorize(alice, "", actions).get(0));

    ResourcePattern wildcard = new ResourcePattern("Topic", "*", PatternType.LITERAL);
    updateRoleBinding(alice, "Reader", clusterA, Collections.singleton(wildcard));
    assertEquals(AuthorizeResult.ALLOWED, authorizer.authorize(alice, "", actions).get(0));
  }

  private void verifyResourceAccessRules(ResourcePattern roleResource) {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();
    Set<ResourcePattern> resources = roleResource == null ?
        Collections.emptySet() : Collections.singleton(roleResource);

    updateRoleBinding(alice, "Reader", clusterA, resources);
    verifyRules(alice, emptyGroups, clusterA, clusterResource);
    verifyRules(alice, emptyGroups, clusterA, topic, "Read", "Describe");

    updateRoleBinding(admin, "Writer", clusterA, resources);
    verifyRules(alice, groups, clusterA, topic, "Read", "Describe", "Write");

    updateRoleBinding(alice, "Writer", clusterA, resources);
    verifyRules(alice, groups, clusterA, topic, "Read", "Describe", "Write");

    deleteRoleBinding(admin, "Writer", clusterA);
    verifyRules(alice, groups, clusterA, topic, "Read", "Describe", "Write");

    deleteRoleBinding(alice, "Reader", clusterA);
    verifyRules(alice, groups, clusterA, topic, "Describe", "Write");

    deleteRoleBinding(alice, "Writer", clusterA);
    verifyRules(alice, groups, clusterA, topic);
  }

  @Test
  public void testScopes() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, clusterResource);

    updateRoleBinding(alice, "ClusterAdmin", clusterB, Collections.emptySet());
    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");

  }

  @Test
  public void testProviderScope() throws Exception {
    initializeRbacProvider("clusterA", Scope.intermediateScope("testOrg"), Collections.emptyMap());

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    updateRoleBinding(alice, "Operator", clusterA, Collections.singleton(topic));
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterA, clusterResource);

    updateRoleBinding(alice, "ClusterAdmin", clusterB, Collections.emptySet());
    verifyRules(alice, groups, clusterA, clusterResource);
    verifyRules(alice, groups, clusterA, topic, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterB, clusterResource, "AlterConfigs", "DescribeConfigs");
    verifyRules(alice, groups, clusterB, topic);

    try {
      Scope anotherScope = new Scope.Builder("anotherOrg").withKafkaCluster("clusterA").build();
      Action action = new Action(anotherScope, clusterResource, new Operation("Describe"));
      rbacProvider.findRule(alice, groups, "", action);
      fail("Did not fail with invalid scope");
    } catch (InvalidScopeException e) {
      // Expected exception
    }
  }

  @Test
  public void testMetadataServer() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT");
    configs.put(MetadataServerConfig.METADATA_SERVER_LISTENERS_PROP, "http://somehost:8095");
    initializeRbacProvider("clusterA", Scope.intermediateScope("testOrg"), configs);
    MockMetadataServer metadataServer = new MockMetadataServer() {
      @Override
      public boolean providerConfigured(Map<String, ?> configs) {
        return true;
      }
    };

    rbacProvider
        .start(KafkaTestUtils.serverInfo("clusterA", metadataServer, SecurityProtocol.PLAINTEXT),
            configs)
        .toCompletableFuture().get();
    assertTrue(rbacProvider.providerConfigured(configs));
    assertTrue(rbacProvider.usesMetadataFromThisKafkaCluster());
    assertEquals(ServerState.REGISTERED, metadataServer.serverState);
  }

  @Test
  public void testMetadataServerConfigs() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT");
    configs.put(MetadataServerConfig.HTTP_SERVER_LISTENERS_PROP, "http://localhost:8091");
    configs.put(MetadataServerConfig.METADATA_SERVER_LISTENERS_PROP, "http://somehost:8095");
    configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "keystore.jks");
    configs.put(MetadataServerConfig.HTTP_SERVER_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "truststore.jks");
    configs.put(MetadataServerConfig.METADATA_SERVER_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mds-ks-password");
    configs.put(MetadataServerConfig.HTTP_SERVER_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "http-ts-password");
    configs.put(MetadataServerConfig.METADATA_SERVER_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "mds-ts-password");
    configs.put(ConfluentConfigs.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8070");

    MetadataServerConfig serverConfig = new MetadataServerConfig(configs);
    assertTrue(serverConfig.isConfluentMetadataServerEnabled());
    assertTrue(serverConfig.isServerEnabled());
    assertEquals(Collections.singletonList(new URL("http://somehost:8095")), serverConfig.listeners());
    assertEquals(Collections.singletonList(new URL("http://somehost:8095")), serverConfig.metadataServerAdvertisedListeners());
    assertEquals("http://somehost:8095", serverConfig.serverConfigs().get("listeners"));
    assertEquals("http://somehost:8095", serverConfig.serverConfigs().get("advertised.listeners"));
    assertEquals("http://somehost:8095", serverConfig.serverConfigs().get(MetadataServerConfig.METADATA_SERVER_LISTENERS_PROP));
    assertNull(serverConfig.serverConfigs().get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("truststore.jks", serverConfig.serverConfigs().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("mds-ks-password", serverConfig.serverConfigs().get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    assertEquals("mds-ts-password", serverConfig.serverConfigs().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    assertEquals("http://localhost:8070", serverConfig.serverConfigs().get(ConfluentConfigs.SCHEMA_REGISTRY_URL_CONFIG));
  }

  @Test
  public void testMetadataServerDisabled() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT");
    configs.put(MetadataServerConfig.HTTP_SERVER_LISTENERS_PROP, "http://localhost:8091,https://localhost:8092");
    configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "keystore.jks");
    configs.put(MetadataServerConfig.HTTP_SERVER_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "truststore.jks");
    configs.put(MetadataServerConfig.METADATA_SERVER_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mds-ks-password");
    configs.put(MetadataServerConfig.HTTP_SERVER_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "http-ts-password");
    configs.put(MetadataServerConfig.METADATA_SERVER_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "mds-ts-password");

    MetadataServerConfig serverConfig = new MetadataServerConfig(configs);
    assertFalse(serverConfig.isConfluentMetadataServerEnabled());
    assertTrue(serverConfig.isServerEnabled());
    assertEquals(Arrays.asList(new URL("http://localhost:8091"), new URL("https://localhost:8092")),
        serverConfig.listeners());
    assertEquals(Collections.emptyList(), serverConfig.metadataServerAdvertisedListeners());
    assertEquals("http://localhost:8091,https://localhost:8092", serverConfig.serverConfigs().get("listeners"));
    assertNull("keystore.jks", serverConfig.serverConfigs().get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("truststore.jks", serverConfig.serverConfigs().get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertNull(serverConfig.serverConfigs().get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    assertEquals("http-ts-password", serverConfig.serverConfigs().get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));

    configs = new HashMap<>();
    configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT");
    configs.put("listeners", "http://localhost:9092");
    serverConfig = new MetadataServerConfig(configs);
    assertEquals(Collections.singletonList(new URL(MetadataServerConfig.HTTP_SERVER_LISTENERS_DEFAULT)), serverConfig.listeners());
    assertEquals(MetadataServerConfig.HTTP_SERVER_LISTENERS_DEFAULT, serverConfig.serverConfigs().get("listeners"));
  }

  private void initializeRbacProvider(String clusterId, Scope authStoreScope, Map<String, ?> configs) throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    MockAuthStore authStore = new MockAuthStore(rbacRoles, authStoreScope);
    authCache = authStore.authCache();
    aclClientOp = Optional.of(new TestMdsAdminClient(Collections.singletonList(new Node(1, "localhost", 9092))));
    rbacProvider = new ConfluentProvider() {
      @Override
      public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaTestUtils.setFinalField(rbacProvider, ConfluentProvider.class, "authCache", authCache);
      }

      @Override
      protected ConfluentAdmin createMdsAdminClient(AuthorizerServerInfo serverInfo, Map<String, ?> clientConfigs) {
        return aclClientOp.get();
      }

      @Override
      protected AuthStore createAuthStore(Scope scope, AuthorizerServerInfo serverInfo, Map<String, ?> configs) {
        return new MockAuthStore(RbacRoles.loadDefaultPolicy(), scope);
      }
    };
    rbacProvider.onUpdate(new ClusterResource(clusterId));
    rbacProvider.configure(configs);
  }

  private void updateRoleBinding(KafkaPrincipal principal, String role, Scope scope, Set<ResourcePattern> resources) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    RoleBindingValue value = new RoleBindingValue(resources == null ? Collections.emptySet() : resources);
    authCache.put(key, value);
  }

  private void deleteRoleBinding(KafkaPrincipal principal, String role, Scope scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    authCache.remove(key);
  }

  private void verifyRules(KafkaPrincipal userPrincipal,
                           Set<KafkaPrincipal> groupPrincipals,
                           Scope scope,
                           ResourcePattern resource,
                           String... expectedOps) {
    RbacTestUtils.verifyPermissions(rbacProvider, userPrincipal, groupPrincipals, scope, resource, expectedOps);
  }

  private void verifyAccess(EmbeddedAuthorizer authorizer, KafkaPrincipal principal,
      Scope scope, ResourceType resourceType, Operation op, AuthorizeResult expectedResult) {
    Action action = new Action(scope, resourceType, "name", op);
    assertEquals(expectedResult,
        authorizer.authorize(principal, "localhost", Collections.singletonList(action)).get(0));
  }

  @Test
  public void testLiteralResourceAclRules() {
    verifyAclRules(new ResourcePattern("Topic", topic.name(), PatternType.LITERAL));
  }

  @Test
  public void testWildcardResourceAclRules() {
    verifyAclRules(new ResourcePattern("Topic", "*", PatternType.LITERAL));
  }

  @Test
  public void testPrefixedResourceAclRules() {
    verifyAclRules(new ResourcePattern("Topic", "top", PatternType.PREFIXED));
  }

  @Test
  public void testSingleCharPrefixedResourceAclRules() {
    verifyAclRules(new ResourcePattern("Topic", "t", PatternType.PREFIXED));
  }

  @Test
  public void testFullNamePrefixedResourceAclRules() {
    verifyAclRules(new ResourcePattern("Topic", "topic", PatternType.PREFIXED));
  }

  private void verifyAclRules(ResourcePattern resourcePattern) {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();

    List<AclRule> accessRules = new LinkedList<>();
    accessRules.add(new AclRule(alice, PermissionType.ALLOW, "", new Operation("Read")));
    updateAclBinding(clusterA, resourcePattern, accessRules);
    verifyRules(alice, emptyGroups, clusterA, clusterResource);
    verifyRules(alice, emptyGroups, clusterA, topic, "Read");

    accessRules.add(new AclRule(admin, PermissionType.ALLOW, "", new Operation("Write")));
    updateAclBinding(clusterA, resourcePattern, accessRules);
    verifyRules(alice, groups, clusterA, topic,  "Write", "Read");

    accessRules.add(new AclRule(alice, PermissionType.ALLOW, "", new Operation("Write")));
    updateAclBinding(clusterA, resourcePattern, accessRules);
    verifyRules(alice, emptyGroups, clusterA, topic, "Write", "Read");

    deleteAclBinding(clusterA, resourcePattern);
    verifyRules(alice, groups, clusterA, topic);
  }

  private void updateAclBinding(Scope scope,
                                ResourcePattern resourcePattern,
                                List<AclRule> accessRule) {
    AclBindingKey key = new AclBindingKey(resourcePattern, scope);
    AclBindingValue value = new AclBindingValue(accessRule);
    authCache.put(key, value);
  }

  private void deleteAclBinding(Scope scope, ResourcePattern resourcePattern) {
    AclBindingKey key = new AclBindingKey(resourcePattern, scope);
    authCache.remove(key);
  }

  @Test
  public void testLiteralResourceWithAclClient() {
    verifyWithAclClient(new ResourcePattern("Topic", topic.name(), PatternType.LITERAL));
  }

  @Test
  public void testWildcardResourceWithAclClient() {
    verifyWithAclClient(new ResourcePattern("Topic", "*", PatternType.LITERAL));
  }

  @Test
  public void testPrefixedResourceWithAclClient() {
    verifyWithAclClient(new ResourcePattern("Topic", "top", PatternType.PREFIXED));
  }

  @Test
  public void testSingleCharPrefixedResourceWithAclClient() {
    verifyWithAclClient(new ResourcePattern("Topic", "t", PatternType.PREFIXED));
  }

  @Test
  public void testFullNamePrefixedResourceWithAclClient() {
    verifyWithAclClient(new ResourcePattern("Topic", "topic", PatternType.PREFIXED));
  }

  private void verifyWithAclClient(ResourcePattern resourcePattern) {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();

    TestMdsAdminClient aclClient = aclClientOp.get();

    AclRule aliceReadRule = new AclRule(alice, PermissionType.ALLOW, "", new Operation("Read"));
    AclBinding aclBinding = new AclBinding(ResourcePattern.to(resourcePattern), aliceReadRule.toAccessControlEntry());
    aclClient.createCentralizedAcls(Collections.singletonList(aclBinding), new CreateAclsOptions(), "clusterA", 0);
    verifyRules(alice, emptyGroups, clusterA, clusterResource);
    verifyRules(alice, emptyGroups, clusterA, topic, "Read");

    AclRule adminWriteReadRule = new AclRule(admin, PermissionType.ALLOW, "", new Operation("Write"));
    aclBinding = new AclBinding(ResourcePattern.to(resourcePattern), adminWriteReadRule.toAccessControlEntry());
    aclClient.createCentralizedAcls(Collections.singletonList(aclBinding), new CreateAclsOptions(), "clusterA", 0);
    verifyRules(alice, groups, clusterA, topic,  "Write");

    AclRule aliceWriteRule = new AclRule(alice, PermissionType.ALLOW, "", new Operation("Write"));
    aclBinding = new AclBinding(ResourcePattern.to(resourcePattern), aliceWriteRule.toAccessControlEntry());

    aclClient.createCentralizedAcls(Collections.singletonList(aclBinding), new CreateAclsOptions(), "clusterA", 0);
    verifyRules(alice, emptyGroups, clusterA, topic, "Write");

    AclBindingFilter deleteFilter = new AclBindingFilter(ResourcePattern.to(resourcePattern).toFilter(),
        AccessControlEntryFilter.ANY);
    aclClient.deleteCentralizedAcls(Collections.singleton(deleteFilter), new DeleteAclsOptions(), "clusterA", 0);
    verifyRules(alice, groups, clusterA, topic);
  }

  private class TestMdsAdminClient extends MockAdminClient implements ConfluentAdmin {

    public TestMdsAdminClient(List<Node> nodes) {
      super(nodes, nodes.get(0));
    }

    @Override
    public ReplicaStatusResult replicaStatus(Set<TopicPartition> partitions,
        ReplicaStatusOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CreateAclsResult createCentralizedAcls(Collection<AclBinding> acls,
        CreateAclsOptions options, String clusterId, int writerBrokerId) {
      Scope scope = new Scope(Collections.singletonList("testOrg"), Collections.singletonMap("kafka-cluster", clusterId));
      for (AclBinding aclBinding: acls) {
        updateAclBinding(scope, ResourcePattern.from(aclBinding.pattern()),
            Collections.singletonList(AclRule.from(aclBinding)));
      }
      return null; // result is not used in the tests
    }

    @Override
    public DeleteAclsResult deleteCentralizedAcls(Collection<AclBindingFilter> filters,
        DeleteAclsOptions options, String clusterId, int writerBrokerId) {
      Map<AclBindingFilter, Collection<AclBinding>> toBeDeleted = new HashMap<>();
      Scope scope = new Scope(Collections.singletonList("testOrg"), Collections.singletonMap("kafka-cluster", clusterId));
      for (AclBindingFilter aclBindingFilter: filters) {
        toBeDeleted.put(aclBindingFilter, describeAcls(scope, aclBindingFilter));
        ResourcePattern resourcePattern = ResourcePattern.from(aclBindingFilter.patternFilter());
        deleteAclBinding(scope, resourcePattern);
      }
      return null; // result is not used in the tests
    }

    public Collection<AclBinding> describeAcls(final Scope scope, final AclBindingFilter filter) {
      return authCache.aclBindings(clusterA, filter, r -> true);
    }
  }
}

