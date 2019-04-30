// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.AuthWriter;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.rbac.RbacRoles;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MockRbacProvider extends RbacProvider implements MetadataProvider {

  @Override
  public String providerName() {
    return "MOCK_RBAC";
  }

  @Override
  protected AuthStore createAuthStore(Scope scope, Map<String, ?> configs) {
    return new MockAuthStore(RbacRoles.loadDefaultPolicy(), scope);
  }

  public static class MockAuthStore implements AuthStore {

    private final Scope scope;
    private final Collection<URL> activeNodes;
    private volatile DefaultAuthCache authCache;

    MockAuthStore(RbacRoles rbacRoles, Scope scope) {
      this.scope = scope;
      this.authCache = new DefaultAuthCache(rbacRoles, scope);
      this.activeNodes = new HashSet<>();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public CompletionStage<Void> startReader() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void startService(Collection<URL> serverUrls) {
    }

    @Override
    public DefaultAuthCache authCache() {
      return authCache;
    }

    @Override
    public AuthWriter writer() {
      return null;
    }

    @Override
    public boolean isMasterWriter() {
      return false;
    }

    @Override
    public URL masterWriterUrl(String protocol) {
      return null;
    }

    @Override
    public Collection<URL> activeNodeUrls(String protocol) {
      return activeNodes;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
