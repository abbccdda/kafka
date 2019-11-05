// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.security.authorizer.acl.AclMapper;
import io.confluent.kafka.security.authorizer.acl.AclProvider;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import io.confluent.security.authorizer.AclMigrationAware;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.Provider;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import kafka.security.auth.Operation;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import kafka.security.authorizer.AuthorizerUtils;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

public class ConfluentServerAuthorizer extends EmbeddedAuthorizer implements Authorizer, Reconfigurable {

  private static final Set<String> UNSCOPED_PROVIDERS =
      Utils.mkSet(AccessRuleProviders.ACL.name(), AccessRuleProviders.MULTI_TENANT.name());

  private AclUpdater aclUpdater;

  @Override
  public Set<String> reconfigurableConfigs() {
    if (auditLogProvider() != null)
      return auditLogProvider().reconfigurableConfigs();
    else
      return Collections.emptySet();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    if (auditLogProvider() != null)
      auditLogProvider().validateReconfiguration(configs);
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    if (auditLogProvider() != null)
      auditLogProvider().reconfigure(configs);
  }

  // Visibility for tests
  public void configureServerInfo(AuthorizerServerInfo serverInfo) {
    super.configureServerInfo(serverInfo);

    initializeAclUpdater();

    // Embedded authorizer used in metadata server can use an empty scope since scopes used
    // in authorization are provided by the remote client. For broker authorizer, the scope
    // of the cluster is required if using providers other than ACL providers.
    if (scope().clusters().isEmpty()) {
      Set<String> scopedProviders = accessRuleProviders().stream()
          .map(Provider::providerName)
          .filter(a -> !UNSCOPED_PROVIDERS.contains(a))
          .collect(Collectors.toSet());

      if (!scopedProviders.isEmpty())
        throw new ConfigException("Scope not provided for broker providers: " + scopedProviders);
    }
  }

  private void initializeAclUpdater() {
    Optional<Authorizer> zkAclProvider = zkAclProvider();
    Optional<Authorizer> centralizedAclProvider = centralizedAclProvider();

    if (authorizerConfig.migrateAclsFromZK) {
      if (!zkAclProvider.isPresent()) {
        throw new IllegalArgumentException("Acl migration from ZK to metadata service is enabled," +
            " but AclProvider is not enabled.");
      }

      if (!centralizedAclProvider.isPresent()) {
        throw new IllegalArgumentException("Acl migration from ZK to metadata service is enabled," +
            " but centralized authorizer/RbacProvider is not enabled.");
      }

      if (!(centralizedAclProvider.get() instanceof AclMigrationAware)) {
        throw new IllegalArgumentException("Acl migration from ZK to metadata service is enabled," +
            " but centralized authorizer is not Acl migration aware");
      }

      aclUpdater = new AclUpdater(zkAclProvider, centralizedAclProvider, true);
    } else {
      aclUpdater = new AclUpdater(zkAclProvider, centralizedAclProvider, false);
    }
  }

  // Allow override for testing
  protected Optional<Authorizer> zkAclProvider() {
    return accessRuleProviders().stream()
        .filter(a -> a instanceof AclProvider)
        .findFirst()
        .map(a -> (Authorizer) a);
  }

  // Allow override for testing
  protected Optional<Authorizer> centralizedAclProvider() {
    return accessRuleProviders().stream()
        .filter(a -> !(a instanceof AclProvider))
        .filter(a -> a instanceof Authorizer)
        .findFirst()
        .map(a -> (Authorizer) a);
  }

  @Override
  public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
    configureServerInfo(serverInfo);
    Runnable migrationTask = createMigrationTask();
    CompletableFuture<Void> startFuture = super.start(
        serverInfo,
        ConfluentConfigs.interBrokerClientConfigs(authorizerConfig, serverInfo.interBrokerEndpoint()),
        migrationTask);

    Map<Endpoint, CompletableFuture<Void>> futures = new HashMap<>(serverInfo.endpoints().size());
    Optional<String> controlPlaneListener = Optional.ofNullable((String)
        authorizerConfig.originals().get(KafkaConfig$.MODULE$.ControlPlaneListenerNameProp()));

    // On brokers that are not running MDS, super.start() returns only after metadata is available
    // and startFuture is complete. On brokers running MDS, startFuture may not be complete, but we
    // should allow control plane and inter-broker listeners to start up in order to process metadata.
    serverInfo.endpoints().forEach(endpoint -> {
      if (endpoint.equals(serverInfo.interBrokerEndpoint()) || endpoint.listenerName().equals(controlPlaneListener)) {
        futures.put(endpoint, CompletableFuture.completedFuture(null));
      } else {
        futures.put(endpoint, startFuture);
      }
    });
    return futures;
  }

  private Runnable createMigrationTask() {
    if (authorizerConfig.migrateAclsFromZK) {
      return ((AclMigrationAware) aclUpdater.centralizedAclAuthorizer.get())
          .migrationTask(aclUpdater.zkAclAuthorizer.get());
    }
    return () -> { };
  }

  @Override
  public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext,
      List<org.apache.kafka.server.authorizer.Action> actions) {
    return actions.stream().map(action -> {
      boolean allowed = authorize(requestContext, action);
      return allowed ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED;
    }).collect(Collectors.toList());
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(
      AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
    return createAcls(requestContext, aclBindings, Optional.empty());
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(
      AuthorizableRequestContext requestContext,
      List<AclBinding> aclBindings,
      Optional<String> clusterId) {
    try {
      return aclUpdater.createAcls(requestContext, aclBindings, clusterId);
    } catch (Throwable t) {
      log.error("createAcls failed", t);
      throw t;
    }
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
      AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
    return deleteAcls(requestContext, aclBindingFilters, Optional.empty());
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
      AuthorizableRequestContext requestContext,
      List<AclBindingFilter> aclBindingFilters,
      Optional<String> clusterId) {
    try {
      return aclUpdater.deleteAcls(requestContext, aclBindingFilters, clusterId);
    } catch (Throwable t) {
      log.error("deleteAcls failed", t);
      throw t;
    }
  }

  @Override
  public Iterable<AclBinding> acls(AclBindingFilter filter) {
    return aclUpdater.acls(filter);
  }

  private boolean authorize(AuthorizableRequestContext requestContext, org.apache.kafka.server.authorizer.Action kafkaAction) {

    Operation operation = Operation$.MODULE$.fromJava(kafkaAction.operation());
    Resource resource = AuthorizerUtils.convertToResource(kafkaAction.resourcePattern());
    if (resource.patternType() != PatternType.LITERAL) {
      throw new IllegalArgumentException("Only literal resources are supported, got: "
          + resource.patternType());
    }

    if (allowBrokerUsersOnInterBrokerListener(requestContext, requestContext.principal())) {
      return true;
    }

    ResourcePattern resourcePattern = new ResourcePattern(AclMapper.resourceType(resource.resourceType()),
        resource.name(), PatternType.LITERAL);
    Action action = new Action(scope(),
                               resourcePattern,
                               AclMapper.operation(operation),
                               kafkaAction.resourceReferenceCount(),
                               kafkaAction.logIfAllowed(),
                               kafkaAction.logIfDenied());

    List<AuthorizeResult> result = super.authorize(
        io.confluent.security.authorizer.utils.AuthorizerUtils.kafkaRequestContext(requestContext),
        Collections.singletonList(action));
    return result.get(0) == AuthorizeResult.ALLOWED;
  }

  private boolean allowBrokerUsersOnInterBrokerListener(AuthorizableRequestContext requestContext, KafkaPrincipal principal) {
    if (interBrokerListener.equals(requestContext.listenerName()) && brokerUsers.contains(principal)) {
      log.debug("principal = {} is a broker user, allowing operation without checking any providers.", principal);
      return true;
    }
    return false;
  }

  /**
   * ACL updater for the authorizer works in different modes depending on the configured
   * access rule providers and migration settings:
   * <ul>
   *   <li>providers=ACL : zkAclAuthorizer is present, centralizedAclAuthorizer not present.
   *       Request without cluster-id: zkAclAuthorizer updates ZK
   *       Request with cluster-id: Fail</li>
   *  <li>providers=RBAC : zkAclAuthorizer not present, centralizedAclAuthorizer is present.
   *      Request without cluster-id: centralizedAclAuthorizer sends request including this
   *      authorizer's cluster-id to the master writer of the metadata cluster.
   *      Request with cluster-id: If master writer, centralizedAclAuthorizer updates metadata
   *      topic. If not, fails.
   *  <li>providers=ACL,RBAC, migrateFromZk=false : zkAclAuthorizer and centralizedAclAuthorizer are present
   *      Request without cluster-id: zkAclAuthorizer updates ZK
   *      Request with cluster-id: If master writer, centralizedAclAuthorizer updates metadata
   *      topic. If not, fails.
   *  <li>providers=ACL,RBAC, migrateFromZk=true : zkAclAuthorizer and centralizedAclAuthorizer are present
   *      Request without cluster-id: zkAclAuthorizer updates ZK. And centralizedAclAuthorizer sends
   *      request including this authorizer's cluster-id to the master writer of the metadata cluster.
   *      Request with cluster-id: If master writer, centralizedAclAuthorizer updates metadata
   *      topic. If not, fails.
   *  <li>providers don't contain ACL or RBAC: zkAclAuthorizer and centralizedAclAuthorizer are empty
   *      ACL update requests are failed.
   * </ul>
   */
  private static class AclUpdater {
    private static final InvalidRequestException ACLS_DISABLED =
        new InvalidRequestException("ACL-based authorization is disabled");
    private static final InvalidRequestException CENTRALIZED_ACLS_DISABED =
        new InvalidRequestException("Centralized ACL-based authorization is disabled");


    private final Optional<Authorizer> zkAclAuthorizer;
    private final Optional<Authorizer> centralizedAclAuthorizer;
    private final boolean migrateFromZk;

    AclUpdater(Optional<Authorizer> zkAclAuthorizer,
               Optional<Authorizer> centralizedAclAuthorizer,
               boolean migrateFromZk) {
      this.zkAclAuthorizer = zkAclAuthorizer;
      this.centralizedAclAuthorizer = centralizedAclAuthorizer;
      this.migrateFromZk = migrateFromZk;
    }

    public List<? extends CompletionStage<AclCreateResult>> createAcls(
        AuthorizableRequestContext requestContext,
        List<AclBinding> aclBindings,
        Optional<String> clusterId) {

      List<? extends CompletionStage<AclCreateResult>> createResults = null;
      ensureAclsEnabled(clusterId.isPresent());

      if (zkAclAuthorizer.isPresent() && !clusterId.isPresent()) {
        createResults = zkAclAuthorizer.get().createAcls(requestContext, aclBindings);
        try {
          for (CompletionStage<AclCreateResult> c : createResults) {
            AclCreateResult createResult = (AclCreateResult) ((CompletionStage) c)
                .toCompletableFuture().get();
            if (createResult.exception().isPresent()) {
              return createResults;
            }
          }
        } catch (Exception e) {
          return createResults;
        }
      }
      if (migrateFromZk || !zkAclAuthorizer.isPresent() || clusterId.isPresent()) {
        List<? extends CompletionStage<AclCreateResult>> centralizedResults =
            centralizedAclAuthorizer.get().createAcls(requestContext, aclBindings, clusterId);
        return createResults == null ? centralizedResults : createResults;
      } else
        return createResults;
    }

    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
        AuthorizableRequestContext requestContext,
        List<AclBindingFilter> aclBindingFilters,
        Optional<String> clusterId) {

      List<? extends CompletionStage<AclDeleteResult>> deleteResults = null;
      ensureAclsEnabled(clusterId.isPresent());

      if (zkAclAuthorizer.isPresent() && !clusterId.isPresent()) {
        deleteResults = zkAclAuthorizer.get().deleteAcls(requestContext, aclBindingFilters);
        try {
          for (CompletionStage<AclDeleteResult> c : deleteResults) {
            AclDeleteResult deleteResult = (AclDeleteResult) ((CompletionStage) c)
                .toCompletableFuture().get();
            if (deleteResult.exception().isPresent()) {
              return deleteResults;
            }
          }
        } catch (Exception e) {
          return deleteResults;
        }
      }
      if (migrateFromZk || !zkAclAuthorizer.isPresent() || clusterId.isPresent()) {
        List<? extends CompletionStage<AclDeleteResult>> centralizedResults =
            centralizedAclAuthorizer.get().deleteAcls(requestContext, aclBindingFilters, clusterId);
        return deleteResults == null ? centralizedResults : deleteResults;
      } else
        return deleteResults;
    }

    public Iterable<AclBinding> acls(AclBindingFilter filter) {
      if (zkAclAuthorizer.isPresent())
        return zkAclAuthorizer.get().acls(filter);
      else if (centralizedAclAuthorizer.isPresent())
        return centralizedAclAuthorizer.get().acls(filter);
      else
        throw ACLS_DISABLED;
    }

    private void ensureAclsEnabled(boolean clusterIdProvided) {
      if (!zkAclAuthorizer.isPresent() && !centralizedAclAuthorizer.isPresent())
        throw ACLS_DISABLED;
      if (clusterIdProvided && !centralizedAclAuthorizer.isPresent())
        throw CENTRALIZED_ACLS_DISABED;
    }
  }
}
