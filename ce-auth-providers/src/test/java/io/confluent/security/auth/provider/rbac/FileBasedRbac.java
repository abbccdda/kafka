// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.AuthWriter;
import io.confluent.security.auth.metadata.MetadataServer;
import io.confluent.security.authorizer.Authorizer;
import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.rbac.RoleBinding;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RBAC provider for use in system tests. This is used to load roles from a file
 * during start up in system tests since we don't have a real Metadata Server in ce-kafka.
 */
public class FileBasedRbac extends RbacProvider {
  private static final Logger log = LoggerFactory.getLogger(FileBasedRbac.class);

  private static final String PROVIDER_NAME = "FILE_RBAC";
  private static final String FILENAME_PROP = "test.metadata.rbac.file";
  private static final int TIMEOUT_MS = 300000;

  public static class Provider extends RbacProvider {
    @Override
    public String providerName() {
      return PROVIDER_NAME;
    }
  }

  public static class Server extends Thread implements MetadataServer {
    private volatile AuthStore authStore;
    private volatile File bindingsFile;

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
    public void start(Authorizer embeddedAuthorizer,
                      AuthStore authStore,
                      AuthenticateCallbackHandler callbackHandler) {
      this.authStore = authStore;
      this.start();
    }

    @Override
    public String providerName() {
      return PROVIDER_NAME;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void run() {
      try {
        long endMs = System.currentTimeMillis() + TIMEOUT_MS;

        while (System.currentTimeMillis() < endMs && !bindingsFile.exists())
          Thread.sleep(1);
        if (!bindingsFile.exists())
          throw new RuntimeException("Role bindings file " + bindingsFile + " not found within timeout");

        while (System.currentTimeMillis() < endMs && !authStore.isMasterWriter())
          Thread.sleep(1);
        if (!authStore.isMasterWriter())
          throw new RuntimeException("Timed out waiting to be elected master writer");

        AuthWriter writer = authStore.writer();
        ObjectMapper objectMapper = JsonMapper.objectMapper();
        RoleBinding[] roleBindings = objectMapper.readValue(bindingsFile, RoleBinding[].class);
        for (RoleBinding binding : roleBindings) {
          if (binding.resources().isEmpty())
            writer.addClusterRoleBinding(binding.principal(), binding.role(), binding.scope()).toCompletableFuture().get();
          else
            writer.replaceResourceRoleBinding(binding.principal(), binding.role(), binding.scope(), binding.resources()).toCompletableFuture().get();
          log.debug("Created role binding {}", binding);
        }
        log.info("Completed loading RBAC role bindings from {}", bindingsFile);
        if (bindingsFile.delete())
          log.debug("Updated role bindings and deleted bindings file");
        else
          log.error("Role bindings file could not be deleted");

      } catch (Exception e) {
        log.error("Role bindings could not be loaded from " + bindingsFile, e);
      }
    }
  }
}
