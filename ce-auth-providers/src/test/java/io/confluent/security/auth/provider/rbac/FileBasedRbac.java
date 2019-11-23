// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.AuthWriter;
import io.confluent.security.auth.provider.ConfluentProvider;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Authorizer;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.rbac.RoleBinding;
import io.confluent.security.store.NotMasterWriterException;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.http.MetadataServer;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RBAC provider for use in system tests. This is used to load roles from a file
 * during start up in system tests since we don't have a real Metadata Server in ce-kafka.
 */
public class FileBasedRbac extends ConfluentProvider {
  private static final Logger log = LoggerFactory.getLogger(FileBasedRbac.class);

  private static final String PROVIDER_NAME = "FILE_RBAC";
  private static final String FILENAME_PROP = "test.metadata.rbac.file";

  public static class Provider extends ConfluentProvider {
    @Override
    public String providerName() {
      return PROVIDER_NAME;
    }
  }

  public static class Server extends Thread implements MetadataServer {
    private volatile Authorizer authorizer;
    private volatile AuthStore authStore;
    private volatile File bindingsFile;
    private volatile boolean isAlive;

    public Server() {
      this.setDaemon(true);
      this.setName("test-metadata-server");
    }

    @Override
    public void configure(Map<String, ?> configs) {
      String bindingsPath = (String) configs.get(FILENAME_PROP);
      if (bindingsPath == null)
        throw new ConfigException("RBAC bindings file not specified");
      bindingsFile = new File(bindingsPath);
    }

    @Override
    public String serverName() {
      return PROVIDER_NAME;
    }

    @Override
    public void registerMetadataProvider(String providerName, MetadataServer.Injector injector) {
      if (providerName.equals(PROVIDER_NAME)) {
        this.authorizer = injector.getInstance(Authorizer.class);
        this.authStore = injector.getInstance(AuthStore.class);
      }
    }

    @Override
    public synchronized void start() {
      isAlive = true;
      super.start();
    }

    @Override
    public void close() {
      isAlive = false;
    }

    @Override
    public void run() {
      try {
        while (isAlive) {
          if (!bindingsFile.exists()) {
            Thread.sleep(10);
            continue;
          }

          // Authorization should work regardless of writer elections and broker failures
          verifyAuthorization();
          if (!authStore.isMasterWriter()) {
            Thread.sleep(10);
            continue;
          }
          if (!authStore.isMasterWriter() || !updateRoleBindings(bindingsFile)) {
              log.warn("Role bindings could not be updated, writer re-election may be in progress");
          }
        }
      } catch (Exception e) {
        log.error("Provider failed with unexpected exception", e);
      }
    }

    private boolean updateRoleBindings(File bindingsFile) throws Exception {
      try {
        AuthWriter writer = authStore.writer();
        ObjectMapper objectMapper = JsonMapper.objectMapper();
        RoleBinding[] roleBindings = objectMapper.readValue(bindingsFile, RoleBinding[].class);
        for (RoleBinding binding : roleBindings) {
          if (binding.resources().isEmpty()) {
            writer.addClusterRoleBinding(binding.principal(), binding.role(), binding.scope())
                .toCompletableFuture().get();
          } else {
            writer.replaceResourceRoleBinding(binding.principal(), binding.role(), binding.scope(),
                    binding.resources()).toCompletableFuture().get();
          }
          log.debug("Created role binding {}", binding);
        }
        log.info("Completed loading RBAC role bindings from {}", bindingsFile);
        TestUtils.waitForCondition(() -> roleBindingUpdated(roleBindings[0]), "Role binding not updated");
        if (bindingsFile.delete())
          log.debug("Updated role bindings and deleted bindings file");
        else
          log.error("Role bindings file could not be deleted");
        return true;
      } catch (NotMasterWriterException e) {
        log.warn("Writer re-election during role update", e);
        return false;
      } catch (Exception e) {
        log.error("Role bindings could not be loaded from " + bindingsFile, e);
        throw e;
      }
    }

    private boolean roleBindingUpdated(RoleBinding binding) {
      ResourcePattern resource = binding.resources().isEmpty() ? null : binding.resources().iterator().next();
      Action action = new Action(binding.scope(),
          resource != null ? resource.resourceType() : new ResourceType("Cluster"),
          resource != null ? resource.name() : "kafka-cluster",
          new Operation("Describe"));
      AuthorizeResult authorizeResult = authorizer.authorize(binding.principal(), "",
          Collections.singletonList(action)).get(0);
      return authorizeResult == AuthorizeResult.ALLOWED;
    }

    private void verifyAuthorization() {
      Action action = new Action(Scope.kafkaClusterScope("somecluster"),
      new ResourceType("Topic"), "sometopic", new Operation("Read"));
      AuthorizeResult authorizeResult = authorizer.authorize(
          new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "SomeUser"),
          "", Collections.singletonList(action)).get(0);
      if (authorizeResult != AuthorizeResult.DENIED) {
        throw new IllegalStateException("Unexpected authorize result: " + authorizeResult);
      }
    }
  }
}
