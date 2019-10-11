// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.authorizer.AclMigrationAware;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.utils.AuthorizerUtils;
import kafka.server.KafkaConfig$;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AclMigrationTest {

  private EmbeddedZookeeper zookeeper;
  private String zkConnect;

  private final static KafkaPrincipal USER_PRINCIPAL = KafkaPrincipal.ANONYMOUS;

  @Before
  public void setUp() {
    zookeeper = new EmbeddedZookeeper();
    zkConnect = "localhost:" + zookeeper.port();
  }

  @After
  public void tearDown() {
    zookeeper.shutdown();
    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testAclMigrationLogic() throws Exception {
    //Test initialization without RBAC Provider
    Authorizer authorizer1 = new TestAuthorizer();
    Map<String, Object> authorizerConfigs = new HashMap<>();
    authorizerConfigs.put(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
    authorizerConfigs.put(ConfluentAuthorizerConfig.MIGRATE_ACLS_FROM_ZK_PROP, "true");
    authorizer1.configure(authorizerConfigs);
    AuthorizerServerInfo serverInfo = KafkaTestUtils.serverInfo("clusterA", SecurityProtocol.SSL);

    assertThrows(IllegalArgumentException.class, () ->
        ((ConfluentServerAuthorizer) authorizer1).configureServerInfo(serverInfo));

    authorizer1.close();

    //Test Initialization without Metadata Service Rest Client Configs
    TestMigrationAuthorizer authorizer2 = new TestMigrationAuthorizer();
    authorizer2.configure(authorizerConfigs);

    assertThrows(IllegalArgumentException.class, () ->
        authorizer2.configureServerInfo(serverInfo));
    authorizer2.close();

    //Test with Metadata service client configs
    TestMigrationAuthorizer authorizer3 = new TestMigrationAuthorizer();
    authorizerConfigs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, "http://locahost:8090");
    authorizer3.configure(authorizerConfigs);
    authorizer3.configureServerInfo(serverInfo);

    //check migrationTask is called
    List<AclBinding> sourceBindings = authorizer3.aclAuthorizer.srcAclCache;
    assertFalse(authorizer3.aclAuthorizer.srcAclCache.isEmpty());


    assertTrue(authorizer3.secondAuthorizer.destAclCache.isEmpty());
    authorizer3.start(serverInfo);
    assertEquals(sourceBindings.size(), authorizer3.secondAuthorizer.destAclCache.size());

    //check Acl updates called on both ZK and RBAC Provider
    AclBinding newBinding1 = aclBinding("test5", AclOperation.DESCRIBE);
    AclBinding newBinding2 = aclBinding("test6", AclOperation.DESCRIBE);
    List<AclBinding> newBindings = new LinkedList<>();
    newBindings.add(newBinding1);
    newBindings.add(newBinding2);

    // test acl creation
    authorizer3.createAcls(newRequestContext(), newBindings);
    assertEquals(sourceBindings.size(), authorizer3.aclAuthorizer.srcAclCache.size());
    assertEquals(sourceBindings.size(), authorizer3.secondAuthorizer.destAclCache.size());
    assertEquals(sourceBindings, authorizer3.secondAuthorizer.destAclCache);

    //test acl deletion
    List<AclBindingFilter> deleteFilters = new LinkedList<>();
    deleteFilters.add(newBinding1.toFilter());
    deleteFilters.add(newBinding2.toFilter());

    authorizer3.deleteAcls(newRequestContext(), deleteFilters);
    assertEquals(sourceBindings.size(), authorizer3.aclAuthorizer.srcAclCache.size());
    assertEquals(sourceBindings.size(), authorizer3.secondAuthorizer.destAclCache.size());
    assertEquals(sourceBindings, authorizer3.secondAuthorizer.destAclCache);

    authorizer3.close();
  }

  private RequestContext newRequestContext() {
    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test");
    return AuthorizerUtils.newRequestContext(RequestContext.KAFKA, principal, "localhost");
  }

  private static class TestAuthorizer extends ConfluentServerAuthorizer {
    volatile AuthorizerServerInfo serverInfo;

    @Override
    public void configure(Map<String, ?> configs) {
      super.configure(configs);
      if (serverInfo != null)
        configureServerInfo(serverInfo);
    }

    @Override
    public void configureServerInfo(AuthorizerServerInfo serverInfo) {
      this.serverInfo = serverInfo;
      super.configureServerInfo(serverInfo);
    }
  }

  private static class TestMigrationAuthorizer extends TestAuthorizer {
    TestAclAuthorizer aclAuthorizer = new TestAclAuthorizer();
    TestSecondAuthorizer secondAuthorizer = new TestSecondAuthorizer();

    @Override
    protected Optional<Authorizer> zkAclProvider() {
      return Optional.of(aclAuthorizer);
    }

    @Override
    protected Optional<Authorizer> centralizedAclProvider() {
      return Optional.of(secondAuthorizer);
    }
  }

  private static class TestAclAuthorizer implements Authorizer {
    List<AclBinding> srcAclCache = new LinkedList<>();

    TestAclAuthorizer() {
      srcAclCache.add(aclBinding("topic1", AclOperation.WRITE));
      srcAclCache.add(aclBinding("topic2", AclOperation.READ));
      srcAclCache.add(aclBinding("topic3", AclOperation.DELETE));
      srcAclCache.add(aclBinding("topic4", AclOperation.DESCRIBE));
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(final AuthorizerServerInfo serverInfo) {
      return null;
    }

    @Override
    public List<AuthorizationResult> authorize(final AuthorizableRequestContext requestContext, final List<Action> actions) {
      return null;
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(final AuthorizableRequestContext requestContext, final List<AclBinding> aclBindings) {
      srcAclCache.addAll(aclBindings);
      return Collections.emptyList();
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(final AuthorizableRequestContext requestContext, final List<AclBindingFilter> aclBindingFilters) {
      deleteBindings(aclBindingFilters, srcAclCache);
      return Collections.emptyList();
    }

    @Override
    public Iterable<AclBinding> acls(final AclBindingFilter filter) {
      return srcAclCache;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final Map<String, ?> configs) {
    }
  }

  private static class TestSecondAuthorizer implements Authorizer, AclMigrationAware {
    List<AclBinding> destAclCache = new LinkedList<>();

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(final AuthorizerServerInfo serverInfo) {
      return null;
    }

    @Override
    public List<AuthorizationResult> authorize(final AuthorizableRequestContext requestContext, final List<Action> actions) {
      return null;
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(final AuthorizableRequestContext requestContext, final List<AclBinding> aclBindings) {
      destAclCache.addAll(aclBindings);
      return Collections.emptyList();
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(final AuthorizableRequestContext requestContext, final List<AclBindingFilter> aclBindingFilters) {
      deleteBindings(aclBindingFilters, destAclCache);
      return Collections.emptyList();
    }

    @Override
    public Iterable<AclBinding> acls(final AclBindingFilter filter) {
      return destAclCache;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final Map<String, ?> configs) {
    }

    @Override
    public Runnable migrationTask(final Authorizer sourceAuthorizer) {
      return () -> {
        Iterable<AclBinding> bindings = sourceAuthorizer.acls(AclBindingFilter.ANY);
        for (AclBinding aclBinding: bindings) {
          destAclCache.add(aclBinding);
        }
      };
    }
  }

  private static AclBinding aclBinding(String topicName, AclOperation operation) {
    return new AclBinding(new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
        new AccessControlEntry(USER_PRINCIPAL.toString(), "*", operation, AclPermissionType.ALLOW));
  }

  private static void deleteBindings(final List<AclBindingFilter> aclBindingFilters, List<AclBinding> aclCache) {
    for (AclBindingFilter aclBindingFilter: aclBindingFilters) {
      for (AclBinding aclBinding: aclCache) {
        if (aclBindingFilter.matches(aclBinding))
          aclCache.remove(aclBinding);
      }
    }
  }
}

