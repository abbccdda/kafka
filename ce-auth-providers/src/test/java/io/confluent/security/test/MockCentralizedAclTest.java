/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.security.test;

import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizerTest;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.provider.ConfluentProvider;
import io.confluent.security.auth.provider.rbac.MockRbacProvider.MockAuthStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AclBindingKey;
import io.confluent.security.auth.store.data.AclBindingValue;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.acl.AclRule;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.rbac.RbacRoles;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.http.MetadataServerConfig;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MockCentralizedAclTest extends ConfluentServerAuthorizerTest {

  @Override
  protected Authorizer createAuthorizer() {
    return new TestAuthorizer();
  }

  /**
   * This is a replacement for AclAuthorizerTest.testEmptyAclThrowsException(). ZK-based ACLs
   * throw exception on empty resource name since we cannot use empty name in ZK path. But we
   * can store empty name in centralized ACLs, so we test that empty resource ACLs work.
   */
  @Test
  public void testEmptyResourceAclDoesNotThrowException() throws Exception {
    try (Authorizer authorizer = createAclAuthorizer()) {
      authorizer.configure(new ConfluentAuthorizerConfig(new Properties()).originals());
      KafkaPrincipal principal =  new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
      InetAddress host = InetAddress.getByName("192.168.0.1");
      RequestContext requestContext = newRequestContext(principal, host, ApiKeys.FIND_COORDINATOR);
      org.apache.kafka.common.resource.ResourcePattern resource =
          new org.apache.kafka.common.resource.ResourcePattern(
              org.apache.kafka.common.resource.ResourceType.GROUP, "", PatternType.LITERAL);
      AccessControlEntry ace = new AccessControlEntry(principal.toString(), host.getHostAddress(),
          AclOperation.DESCRIBE, AclPermissionType.ALLOW);
      AclBinding acl = new AclBinding(resource, ace);
      Action action = new Action(AclOperation.DESCRIBE, resource, 1, true, true);

      assertEquals(AuthorizationResult.DENIED,
          authorizer.authorize(requestContext, Collections.singletonList(action)).get(0));
      authorizer.createAcls(requestContext, Collections.singletonList(acl)).get(0).toCompletableFuture().get();
      assertEquals(AuthorizationResult.ALLOWED,
          authorizer.authorize(requestContext, Collections.singletonList(action)).get(0));
    }
  }

  // Override and ignore ZK-specific tests
  @Ignore
  @Test
  @Override
  public void testEmptyAclThrowsException() {
  }

  @Ignore
  @Test
  @Override
  public void testDistributedConcurrentModificationOfResourceAcls() {
  }

  @Ignore
  @Test
  @Override
  public void testHighConcurrencyModificationOfResourceAcls() {
  }

  @Ignore
  @Test
  @Override
  public void testHighConcurrencyDeletionOfResourceAcls() {
  }

  @Ignore
  @Test
  @Override
  public void testLoadCache() {
  }

  @Ignore
  @Test
  @Override
  public void testChangeListenerTiming() {
  }

  @Ignore
  @Test
  @Override
  public void testThrowsOnAddPrefixedAclIfInterBrokerProtocolVersionTooLow() {
  }

  @Ignore
  @Test
  @Override
  public void testWritesExtendedAclChangeEventIfInterBrokerProtocolNotSet() {
  }

  @Ignore
  @Test
  @Override
  public void testWritesExtendedAclChangeEventWhenInterBrokerProtocolAtLeastKafkaV2() {
  }

  @Ignore
  @Test
  @Override
  public void testWritesLiteralWritesLiteralAclChangeEventWhenInterBrokerProtocolLessThanKafkaV2eralAclChangesForOlderProtocolVersions() {
  }

  @Ignore
  @Test
  @Override
  public void testWritesLiteralAclChangeEventWhenInterBrokerProtocolIsKafkaV2() {
  }

  private static class TestAuthorizer extends ConfluentServerAuthorizer {

    private final AuthorizerServerInfo serverInfo = KafkaTestUtils
        .serverInfo("clusterA", SecurityProtocol.SSL);
    private final MockCentralizedAclProvider centralizedAclProvider = new MockCentralizedAclProvider(
        "clusterA");
    private final Map<String, Object> configs = new HashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
      this.configs
          .put(MetadataServerConfig.METADATA_SERVER_LISTENERS_PROP, "http://127.0.0.1:8090");
      this.configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT");
      this.configs.putAll(configs);
      super.configure(this.configs);
      configureServerInfo(serverInfo);
    }

    @Override
    protected void configureProviders(List<AccessRuleProvider> accessRuleProviders,
        GroupProvider groupProvider,
        MetadataProvider metadataProvider,
        AuditLogProvider auditLogProvider) {
      centralizedAclProvider.onUpdate(serverInfo.clusterResource());
      centralizedAclProvider.configure(configs);
      super.configureProviders(Collections.singletonList(centralizedAclProvider),
          centralizedAclProvider, centralizedAclProvider, auditLogProvider);
      centralizedAclProvider.start(serverInfo, configs);
    }
  }

  private static class MockCentralizedAclProvider extends ConfluentProvider {

    private final String clusterId;
    private final Scope scope;
    private final DefaultAuthCache authCache;

    MockCentralizedAclProvider(String clusterId) {
      this.clusterId = clusterId;
      this.scope = Scope.kafkaClusterScope(clusterId);
      this.authCache = new DefaultAuthCache(RbacRoles.loadDefaultPolicy(), scope);
    }

    @Override
    protected ConfluentAdmin createMdsAdminClient(AuthorizerServerInfo serverInfo,
        Map<String, ?> clientConfigs) {
      Node node = new Node(serverInfo.brokerId(), "localhost", 9092);
      KafkaTestUtils.setFinalField(this, ConfluentProvider.class, "authCache", authCache);
      return EasyMock.createNiceMock(ConfluentAdmin.class);
    }

    @Override
    protected AuthStore createAuthStore(Scope scope, AuthorizerServerInfo serverInfo,
        Map<String, ?> configs) {
      return new MockAuthStore(RbacRoles.loadDefaultPolicy(), scope);
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(
        AuthorizableRequestContext requestContext,
        List<AclBinding> aclBindings,
        Optional<String> aclClusterId) {
      for (AclBinding aclBinding : aclBindings) {
        AclBindingKey key = new AclBindingKey(ResourcePattern.from(aclBinding.pattern()), scope);
        AclBindingValue value = (AclBindingValue) authCache.get(key);
        authCache.put(key, addAcl(value, aclBinding));
      }

      return aclBindings.stream()
          .map(b -> CompletableFuture.completedFuture(new AclCreateResult(null)))
          .collect(Collectors.toList());
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
        AuthorizableRequestContext requestContext,
        List<AclBindingFilter> filters,
        Optional<String> aclClusterId) {

      return filters.stream()
          .map(filter -> {
            List<AclBindingDeleteResult> result = deleteAcls(filter);
            return CompletableFuture.completedFuture(new AclDeleteResult(result));
          }).collect(Collectors.toList());
    }

    public List<AclBindingDeleteResult> deleteAcls(AclBindingFilter filter) {
      Collection<AclBinding> acls = authCache.aclBindings(scope, AclBindingFilter.ANY, r -> true);
      Set<AclBindingKey> allKeys = aclRules(acls, scope).keySet();
      List<AclBindingDeleteResult> deleteResult = new ArrayList<>();
      Collection<AclBinding> toDelete = authCache.aclBindings(scope, filter, r -> true);
      acls.removeAll(toDelete);
      toDelete.forEach(binding -> deleteResult.add(new AclBindingDeleteResult(binding)));

      Map<AclBindingKey, AclBindingValue> remaining = aclRules(acls, scope);
      allKeys.forEach(key -> {
        if (remaining.containsKey(key))
          authCache.put(key, remaining.get(key));
        else
          authCache.remove(key);
      });
      return deleteResult;
    }

    private Map<AclBindingKey, AclBindingValue> aclRules(Collection<AclBinding> acls, Scope scope) {
      Map<AclBindingKey, AclBindingValue> aclMap = new HashMap<>();
      for (AclBinding aclBinding : acls) {
        AclBindingKey key = new AclBindingKey(ResourcePattern.from(aclBinding.pattern()), scope);
        aclMap.put(key, addAcl(aclMap.get(key), aclBinding));
      }
      return aclMap;
    }

    private AclBindingValue addAcl(AclBindingValue existing, AclBinding aclBinding) {
      AclBindingValue value = existing == null ? new AclBindingValue(new ArrayList<>()) : existing;
      value.aclRules().add(AclRule.from(aclBinding));
      return value;
    }
  }
}
