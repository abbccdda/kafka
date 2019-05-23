// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import static org.junit.Assert.fail;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.provider.rbac.RbacProvider;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests default role definitions related to security metadata access (e.g. role management)
 * and authorization with pattern-matching (e.g. MATCH pattern type).
 */
public class MetadataAuthorizationTest {

  private static final Operation DESCRIBE_ACCESS = new Operation("DescribeAccess");
  private static final Operation ALTER_ACCESS = new Operation("AlterAccess");
  private static final Operation DESCRIBE = new Operation("Describe");
  private static final Operation ALTER = new Operation("Alter");
  private static final ResourceType SECURITY_METADATA_TYPE = new ResourceType("SecurityMetadata");
  private static final ResourcePattern SECURITY_METADATA =
      new ResourcePattern(SECURITY_METADATA_TYPE, "security-metadata", PatternType.LITERAL);

  private final Scope clusterA = new Scope.Builder("testOrg").withKafkaCluster("clusterA").build();
  private final Scope clusterB = new Scope.Builder("testOrg").withKafkaCluster("clusterB").build();
  private final KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private final KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
  private final KafkaPrincipal admin = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Admin");

  private final ResourcePattern topicResource = new ResourcePattern(new ResourceType("Topic"), "testtopic", PatternType.LITERAL);
  private final ResourcePattern topicPrefix = new ResourcePattern(new ResourceType("Topic"), "test", PatternType.PREFIXED);
  private final Action topicRead = new Action(clusterA, new ResourceType("Topic"), "testtopic", new Operation("Read"));
  private final Action topicWrite = new Action(clusterA, new ResourceType("Topic"), "testtopic", new Operation("Write"));
  private final Action someTopicOp = new Action(clusterA, new ResourceType("Topic"), "sometopic", new Operation("Write"));

  private EmbeddedAuthorizer authorizer;
  private DefaultAuthCache authCache;

  @Before
  public void setUp() throws Exception {
    authorizer = new EmbeddedAuthorizer();
    Map<String, Object> props = new HashMap<>();
    props.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "MOCK_RBAC");
    props.put(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP, "MOCK_RBAC");
    props.put(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP, "MOCK_RBAC");
    props.put(MetadataServiceConfig.METADATA_SERVER_LISTENERS_PROP, "http://127.0.0.1:8090");
    authorizer.onUpdate(new ClusterResource("clusterA"));
    authorizer.configure(props);
    authorizer.start(Collections.emptyMap()).get();
    RbacProvider rbacProvider = (RbacProvider) authorizer.accessRuleProvider("MOCK_RBAC");
    this.authCache = (DefaultAuthCache) rbacProvider.authStore().authCache();
    RbacTestUtils.updateRoleBinding(authCache, admin, "SystemAdmin", clusterA, Collections.emptySet());
  }

  @After
  public void tearDown() throws Exception {
    if (authorizer != null)
      authorizer.close();
    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testAuthorizeRequest() throws Exception {
    RbacTestUtils.updateRoleBinding(authCache, alice, "DeveloperRead", clusterA, Collections.singleton(topicResource));

    authorizeAuthorizeRequest(admin, topicWrite);
    verifyAuthorizationFailure(() -> authorizeAuthorizeRequest(alice, topicWrite));
    RbacTestUtils.updateRoleBinding(authCache, alice, "UserAdmin", clusterA, Collections.emptySet());
    authorizeAuthorizeRequest(alice, topicWrite);

    authorizeAuthorizeRequest(alice, someTopicOp);
    verifyAuthorizationFailure(() -> authorizeAuthorizeRequest(bob, someTopicOp));
  }

  @Test
  public void testResourceOwnerAuthorize() throws Exception {
    RbacTestUtils.updateRoleBinding(authCache, alice, "ResourceOwner", clusterA, Collections.singleton(topicPrefix));

    authorizeAuthorizeRequest(alice, topicRead);
    verifyAuthorizationFailure(() -> authorizeAuthorizeRequest(alice, someTopicOp));
  }

  @Test
  public void testSecurityMetadataAccess() throws Exception {
    authorizeSecurityMetadataAccess(admin, clusterA, DESCRIBE);
    verifyAuthorizationFailure(() -> authorizeSecurityMetadataAccess(admin, clusterB, DESCRIBE));
    verifyAuthorizationFailure(() -> authorizeSecurityMetadataAccess(alice, clusterA, DESCRIBE));
    RbacTestUtils.updateRoleBinding(authCache, alice, "UserAdmin", clusterA, Collections.emptySet());
    authorizeSecurityMetadataAccess(alice, clusterA, DESCRIBE);
    verifyAuthorizationFailure(() -> authorizeSecurityMetadataAccess(alice, clusterB, DESCRIBE));

    authorizeSecurityMetadataAccess(admin, clusterA, ALTER);
    authorizeSecurityMetadataAccess(alice, clusterA, ALTER);
    verifyAuthorizationFailure(() -> authorizeSecurityMetadataAccess(admin, clusterB, ALTER));
    verifyAuthorizationFailure(() -> authorizeSecurityMetadataAccess(alice, clusterB, ALTER));
  }

  @Test
  public void testResourcePatternAccess() throws Exception {
    RbacTestUtils.updateRoleBinding(authCache, alice, "ResourceOwner", clusterA, Collections.singleton(topicPrefix));
    authorizeResourcePattern(admin, clusterA, topicResource, DESCRIBE_ACCESS);
    authorizeResourcePattern(admin, clusterA, topicPrefix, DESCRIBE_ACCESS);
    authorizeResourcePattern(alice, clusterA, topicResource, DESCRIBE_ACCESS);
    authorizeResourcePattern(alice, clusterA, topicPrefix, DESCRIBE_ACCESS);

    ResourcePattern notMatchingResource = new ResourcePattern(new ResourceType("Topic"), "sometopic", PatternType.LITERAL);
    verifyAuthorizationFailure(() -> authorizeResourcePattern(alice, clusterA, notMatchingResource, DESCRIBE_ACCESS));
    ResourcePattern matchingPrefix = new ResourcePattern(new ResourceType("Topic"), "testt", PatternType.PREFIXED);
    authorizeResourcePattern(alice, clusterA, matchingPrefix, DESCRIBE_ACCESS);
    ResourcePattern notMatchingPrefix = new ResourcePattern(new ResourceType("Topic"), "te", PatternType.PREFIXED);
    verifyAuthorizationFailure(() -> authorizeResourcePattern(alice, clusterA, notMatchingPrefix, DESCRIBE_ACCESS));

    verifyAuthorizationFailure(() -> authorizeResourcePattern(alice, clusterB, topicResource, DESCRIBE_ACCESS));
    verifyAuthorizationFailure(() -> authorizeResourcePattern(alice, clusterB, topicPrefix, DESCRIBE_ACCESS));

    RbacTestUtils.updateRoleBinding(authCache, alice, "UserAdmin", clusterB, Collections.singleton(topicPrefix));
    authorizeResourcePattern(alice, clusterB, topicResource, ALTER_ACCESS);
    authorizeResourcePattern(alice, clusterB, topicPrefix, ALTER_ACCESS);
    authorizeResourcePattern(alice, clusterB, matchingPrefix, ALTER_ACCESS);
    authorizeResourcePattern(alice, clusterB, notMatchingResource, ALTER_ACCESS);
    authorizeResourcePattern(alice, clusterB, notMatchingPrefix, ALTER_ACCESS);
  }

  @Test
  public void testResourcePatternFilterAccess() throws Exception {
    ResourceType topicType = new ResourceType("Topic");
    Collection<ResourcePatternFilter> matching = Arrays.asList(
        new ResourcePatternFilter(topicType, "testtopic1", PatternType.LITERAL),
        new ResourcePatternFilter(topicType, "testt", PatternType.PREFIXED),
        new ResourcePatternFilter(topicType, "testtopic", PatternType.ANY),
        new ResourcePatternFilter(topicType, "testt", PatternType.MATCH)
    );

    Collection<ResourcePatternFilter> notMatching = Arrays.asList(
        new ResourcePatternFilter(topicType, "sometopic", PatternType.LITERAL),
        new ResourcePatternFilter(topicType, "*", PatternType.LITERAL),
        new ResourcePatternFilter(topicType, "te", PatternType.PREFIXED),
        new ResourcePatternFilter(topicType, "te", PatternType.ANY),
        new ResourcePatternFilter(topicType, "t", PatternType.MATCH)
    );

    Collection<ResourcePatternFilter> allTopics = Arrays.asList(
        new ResourcePatternFilter(topicType, "*", PatternType.LITERAL),
        new ResourcePatternFilter(topicType, "*", PatternType.ANY),
        new ResourcePatternFilter(topicType, "*", PatternType.MATCH)
    );

    Collection<ResourcePatternFilter> all = Arrays.asList(
        new ResourcePatternFilter(ResourceType.ALL, "*", PatternType.LITERAL),
        new ResourcePatternFilter(ResourceType.ALL, "*", PatternType.ANY),
        new ResourcePatternFilter(ResourceType.ALL, "*", PatternType.MATCH)
    );

    RbacTestUtils.updateRoleBinding(authCache, alice, "ResourceOwner", clusterA, Collections.singleton(topicPrefix));
    authorizeResourceFilter(alice, clusterA, topicResource.toFilter(), ALTER_ACCESS);
    authorizeResourceFilter(alice, clusterA, topicPrefix.toFilter(), ALTER_ACCESS);
    verifyAuthorizationFailure(() -> authorizeResourceFilter(admin, clusterB, topicResource.toFilter(), ALTER_ACCESS));
    authorizeResourceFilter(admin, clusterA, topicResource.toFilter(), ALTER_ACCESS);
    authorizeResourceFilter(admin, clusterA, topicPrefix.toFilter(), ALTER_ACCESS);

    matching.forEach(filter -> authorizeResourceFilter(admin, clusterA, filter, DESCRIBE_ACCESS));
    notMatching.forEach(filter -> authorizeResourceFilter(admin, clusterA, filter, DESCRIBE_ACCESS));
    all.forEach(filter -> authorizeResourceFilter(admin, clusterA, filter, DESCRIBE_ACCESS));

    matching.forEach(filter -> authorizeResourceFilter(alice, clusterA, filter, ALTER_ACCESS));
    notMatching.forEach(filter -> verifyAuthorizationFailure(() ->
        authorizeResourceFilter(alice, clusterA, filter, ALTER_ACCESS)));
    allTopics.forEach(filter -> verifyAuthorizationFailure(() ->
        authorizeResourceFilter(alice, clusterA, filter, ALTER_ACCESS)));
    all.forEach(filter -> verifyAuthorizationFailure(() ->
        authorizeResourceFilter(alice, clusterA, filter, ALTER_ACCESS)));

    RbacTestUtils.updateRoleBinding(authCache, alice, "UserAdmin", clusterA, Collections.emptySet());
    notMatching.forEach(filter -> authorizeResourceFilter(alice, clusterA, filter, DESCRIBE_ACCESS));
    allTopics.forEach(filter -> authorizeResourceFilter(alice, clusterA, filter, DESCRIBE_ACCESS));
    all.forEach(filter -> authorizeResourceFilter(alice, clusterA, filter, DESCRIBE_ACCESS));

    RbacTestUtils.updateRoleBinding(authCache, bob, "ResourceOwner", clusterA,
        Collections.singleton(new ResourcePattern(topicType, "*", PatternType.LITERAL)));
    notMatching.forEach(filter -> authorizeResourceFilter(bob, clusterA, filter, ALTER_ACCESS));
    allTopics.forEach(filter -> authorizeResourceFilter(bob, clusterA, filter, ALTER_ACCESS));
    all.forEach(filter -> verifyAuthorizationFailure(() ->
        authorizeResourceFilter(bob, clusterA, filter, ALTER_ACCESS)));
  }

  private void authorizeSecurityMetadataAccess(KafkaPrincipal requestorPrincipal, Scope scope, Operation op) {
    authorize(requestorPrincipal, new Action(scope, SECURITY_METADATA, op));
  }

  private void authorizeResourcePattern(KafkaPrincipal requestorPrincipal,
                                        Scope scope,
                                        ResourcePattern resourcePattern,
                                        Operation op) {
    authorize(requestorPrincipal, new Action(scope, resourcePattern, op));
  }

  private void authorizeResourceFilter(KafkaPrincipal requestorPrincipal,
                                       Scope scope,
                                       ResourcePatternFilter filter,
                                       Operation op) {
    ResourcePattern pattern = new ResourcePattern(filter.resourceType(), filter.name(), filter.patternType());
    authorize(requestorPrincipal, new Action(scope, pattern, op));
  }

  private void authorizeAuthorizeRequest(KafkaPrincipal requestorPrincipal, Action action) {
    authorize(requestorPrincipal, new Action(action.scope(), action.resourcePattern(), DESCRIBE_ACCESS));
  }

  private void authorize(KafkaPrincipal principal, Action action) {
    AuthorizeResult result = authorizer.authorize(principal, "", Collections.singletonList(action))
        .get(0);
    if (result != AuthorizeResult.ALLOWED)
      throw new AuthorizationException(action + " not permitted for " + principal);
  }

  private void verifyAuthorizationFailure(Runnable runnable) {
    try {
      runnable.run();
      fail("Authorization did not fail");
    } catch (AuthorizationException e) {
      // expected exception
    }
  }
}
