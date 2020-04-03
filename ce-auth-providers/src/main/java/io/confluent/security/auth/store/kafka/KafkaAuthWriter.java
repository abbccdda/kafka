// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import io.confluent.security.auth.metadata.AuthWriter;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.provider.ldap.LdapStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AclBindingKey;
import io.confluent.security.auth.store.data.AclBindingValue;
import io.confluent.security.auth.store.data.AuthEntryType;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.external.ExternalStore;
import io.confluent.security.authorizer.acl.AclRule;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.authorizer.provider.ProviderFailedException;
import io.confluent.security.authorizer.utils.ThreadUtils;
import io.confluent.security.rbac.AccessPolicy;
import io.confluent.security.rbac.InvalidRoleBindingException;
import io.confluent.security.rbac.Role;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.ConsumerListener;
import io.confluent.security.store.kafka.clients.KafkaPartitionWriter;
import io.confluent.security.store.kafka.clients.KafkaUtils;
import io.confluent.security.store.kafka.clients.StatusListener;
import io.confluent.security.store.kafka.clients.Writer;
import io.confluent.security.store.kafka.coordinator.MetadataServiceRebalanceListener;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Writer that initiates updates to metadata. Each metadata partition has its own partition writer
 * that performs the actual update.
 *
 * Threading model:
 * <ul>
 *   <li>Writer rebalances are processed on a single thread, so startWriter() and stopWriter() are
 *   invoked sequentially, except during shutdown when start() may be in progress</li>
 *   <li>The single-threaded `mgmtExecutor` of this writer is used for asynchronous initialization
 *   and status updates.</li>
 *   <li>The single-threaded `writeExecutor` of this writer is used by this writer as well as
 *   its partition writers to perform actual updates. It also manages request timeouts.</li>
 *   <li>Multiple non-incremental updates may be performed concurrently. Pending writes
 *   are tracked by each KafkaPartitionWriter.</li>
 *   <li>Incremental update request is executed only after all pending writes have completed and
 *   the local cache is up-to-date. Requests are queued in KafkaPartitionWriter until they are ready
 *   to be processed.</li>
 *   <li>ACL update requests are executed on `writeExecutor` to avoid any blocking on the
 *   broker request threads while processing AdminClient requests to update centralized ACLs.</li>
 * </ul>
 */
public class KafkaAuthWriter implements Writer, AuthWriter, ConsumerListener<AuthKey, AuthValue> {

  private static final Logger log = LoggerFactory.getLogger(KafkaAuthWriter.class);

  private final String topic;
  private final int numPartitions;
  private final KafkaStoreConfig config;
  private final Time time;
  private final DefaultAuthCache authCache;
  private final StatusListener statusListener;
  private final Producer<AuthKey, AuthValue> producer;
  private final Supplier<AdminClient> adminClientSupplier;
  private final Map<AuthEntryType, ExternalStore> externalAuthStores;
  private final AtomicBoolean isMasterWriter;
  private final Map<Integer, KafkaPartitionWriter<AuthKey, AuthValue>> partitionWriters;
  private final AtomicBoolean alive;
  private MetadataServiceRebalanceListener rebalanceListener;
  private ExecutorService mgmtExecutor;
  private ScheduledExecutorService writeExecutor;
  private volatile boolean ready;

  public KafkaAuthWriter(String topic,
                         int numPartitions,
                         KafkaStoreConfig config,
                         Producer<AuthKey, AuthValue> producer,
                         Supplier<AdminClient> adminClientSupplier,
                         DefaultAuthCache authCache,
                         StatusListener statusListener,
                         Time time) {
    this.topic = topic;
    this.numPartitions = numPartitions;
    this.config = config;
    this.statusListener = statusListener;
    this.producer = producer;
    this.adminClientSupplier = adminClientSupplier;
    this.authCache = authCache;
    this.time = time;
    this.externalAuthStores = new HashMap<>();
    this.isMasterWriter = new AtomicBoolean();
    this.partitionWriters = new HashMap<>();
    this.alive = new AtomicBoolean(true);
    loadExternalAuthStores();
  }

  @Override
  public void startWriter(int generationId) {
    log.info("Starting writer with generation {}", generationId);

    if (generationId < 0)
      throw new IllegalArgumentException("Invalid generation id for master writer " + generationId);

    if (mgmtExecutor != null && !mgmtExecutor.isTerminated())
      throw new IllegalStateException("Starting writer without clearing startup executor of previous generation");

    isMasterWriter.set(true);

    mgmtExecutor = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("auth-writer-mgmt-%d", true));
    writeExecutor = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("auth-writer-%d", true));
    mgmtExecutor.submit(() -> {
      try {
        if (partitionWriters.isEmpty()) {
          createPartitionWriters();
        }

        StatusValue initializing = new StatusValue(MetadataStoreStatus.INITIALIZING, generationId,
            config.brokerId, null);
        partitionWriters.forEach((partition, writer) ->
            writer.start(generationId, new StatusKey(partition), initializing, writeExecutor));
        ready = true;

      } catch (Throwable e) {
        log.error("Kafka auth writer initialization failed {}, resigning", e);
        rebalanceListener.onWriterResigned(generationId);
      }
    });

    mgmtExecutor.submit(() -> {
      try {
        externalAuthStores.forEach((type, store) -> store.start(generationId));
        updateExternalStatus(MetadataStoreStatus.INITIALIZED, null, generationId);
      } catch (Throwable e) {
        updateExternalStatus(MetadataStoreStatus.FAILED, e.getMessage(), generationId);
      }
    });
  }

  @Override
  public void stopWriter(Integer generationId) {
    try {
      log.info("Stopping writer {}", generationId == null ? "" : "with generation " + generationId);
      ready = false;

      // Shutdown sequence:
      // 1) Shutdown the executor that creates partition writers and external stores first to ensure
      //    that start up operations are aborted.
      // 2) Stop external stores that generate records
      // 3) Stop all partition writers
      // 4) Stop write executors used by partition writers
      if (mgmtExecutor != null)
        mgmtExecutor.shutdownNow();
      if (mgmtExecutor != null) {
        if (!mgmtExecutor.awaitTermination(config.refreshTimeout.toMillis(), TimeUnit.MILLISECONDS))
          throw new TimeoutException("Timed out waiting for start up to be terminated");
      }

      externalAuthStores.values().forEach(store -> store.stop(generationId));
      partitionWriters.values().forEach(KafkaPartitionWriter::stop);

      if (writeExecutor != null)
        writeExecutor.shutdownNow();
      if (writeExecutor != null) {
        if (!writeExecutor.awaitTermination(config.refreshTimeout.toMillis(), TimeUnit.MILLISECONDS))
          throw new TimeoutException("Timed out waiting for start up to be terminated");
      }
    } catch (InterruptedException e) {
      log.debug("Interrupted while shutting down writer executor");
      throw new InterruptException(e);
    } finally {
      mgmtExecutor = null;
      writeExecutor = null;
      isMasterWriter.set(false);

      List<Integer> failedPartitions = partitionWriters.keySet().stream()
          .filter(statusListener::onWriterFailure)
          .collect(Collectors.toList());
      if (!failedPartitions.isEmpty()) {
        String errorMessage = "Partition writers have failed to recover after timeout: " + failedPartitions;
        log.error(errorMessage);
        partitionWriters.keySet().forEach(p -> authCache.fail(p, errorMessage));
      }
    }
  }

  @Override
  public boolean ready() {
    return ready;
  }

  @Override
  public CompletionStage<Void> addClusterRoleBinding(KafkaPrincipal principal, String role, Scope scope) {
    log.debug("addClusterRoleBinding principal={} role={} scope={}", principal, role, scope);
    return replaceResourceRoleBinding(principal, role, scope, Collections.emptySet());
  }

  @Override
  public CompletionStage<Void> addResourceRoleBinding(KafkaPrincipal principal,
                                                String role,
                                                Scope scope,
                                                Collection<ResourcePattern> newResources) {
    log.debug("addResourceRoleBinding principal={} role={} scope={} resources={}", principal, role, scope, newResources);
    validateRoleBindingUpdate(role, scope, newResources, true);
    validateRoleResources(newResources);

    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(key);

    return partitionWriter.update(key,
        existingValue -> {
          Set<ResourcePattern> updatedResources = new HashSet<>();
          if (existingValue != null)
            updatedResources.addAll(((RoleBindingValue) existingValue).resources());
          updatedResources.addAll(newResources);
          log.debug("New binding {} {} {} {}", principal, role, scope, updatedResources);
          return new RoleBindingValue(updatedResources);
        });
  }

  @Override
  public CompletionStage<Void> replaceResourceRoleBinding(KafkaPrincipal principal,
                                                String role,
                                                Scope scope,
                                                Collection<ResourcePattern> resources) {
    log.debug("replaceResourceRoleBinding principal={} role={} scope={} resources={}", principal, role, scope, resources);
    validateRoleBindingUpdate(role, scope, resources, true);
    validateRoleResources(resources);

    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(key);

    return partitionWriter.write(key, new RoleBindingValue(resources), null, true, false);
  }

  @Override
  public CompletionStage<Void> removeRoleBinding(KafkaPrincipal principal, String role, Scope scope) {
    log.debug("removeRoleBinding principal={} role={} scope={}", principal, role, scope);
    validateRoleBindingUpdate(role, scope, Collections.emptySet(), false);

    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(key);

    return partitionWriter.write(key, null, null, true, false);
  }

  @Override
  public CompletionStage<Void> removeResourceRoleBinding(KafkaPrincipal principal,
                                                   String role,
                                                   Scope scope,
                                                   Collection<ResourcePatternFilter> deletedResources) {
    log.debug("removeResourceRoleBinding principal={} role={} scope={} resources={}", principal, role, scope, deletedResources);
    validateRoleBindingUpdate(role, scope, deletedResources, true);

    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(key);

    return partitionWriter.update(key,
        existingValue -> {
          Set<ResourcePattern> updatedResources = new HashSet<>();
          if (existingValue != null)
            updatedResources.addAll(((RoleBindingValue) existingValue).resources());
          deletedResources.forEach(pattern -> updatedResources.removeIf(pattern::matches));
          if (!updatedResources.isEmpty()) {
            log.debug("New binding {} {} {} {}", principal, role, scope, updatedResources);
            return new RoleBindingValue(updatedResources);
          } else {
            log.debug("Deleting binding with no remaining resources {} {} {}", principal, role, scope);
            return null;
          }
        });
  }

  @Override
  public CompletionStage<Void> createAcls(final Scope scope, final AclBinding aclBinding) {
    return createAcls(scope, Collections.singletonList(aclBinding)).get(aclBinding)
        .thenApply(result -> {
          if (result.exception().isPresent())
            throw result.exception().get();
          else
            return null;
        });
  }

  @Override
  public Map<AclBinding, CompletionStage<AclCreateResult>> createAcls(Scope scope, List<AclBinding> aclBindings) {

    log.debug("createAcls scope={} aclBindings={}", scope, aclBindings);
    ensureMasterWriter();
    validateScope(scope);
    aclBindings.forEach(this::validateAclBinding);
    Map<ResourcePattern, List<AclBinding>> bindingsByResource = new HashMap<>();
    aclBindings.forEach(binding -> {
      bindingsByResource.computeIfAbsent(ResourcePattern.from(binding.pattern()),
          unused -> new ArrayList<>()).add(binding);
    });

    Map<AclBinding, CompletionStage<AclCreateResult>> futures = aclBindings.stream()
        .collect(Collectors.toMap(Function.identity(), b -> new CompletableFuture<>()));
    try {
      writeExecutor.submit(() -> {
        try {
          createAcls(scope, bindingsByResource, futures);
        } catch (Throwable t) {
          populateAclCreateFailure(futures, t);
        }
      });
    } catch (Throwable t) {
      populateAclCreateFailure(futures, t);
    }
    return futures;
  }

  private void createAcls(Scope scope,
                          Map<ResourcePattern, List<AclBinding>> aclBindings,
                           Map<AclBinding, CompletionStage<AclCreateResult>> futures) {
    aclBindings.forEach((resourcePattern, bindings) -> {
      AclBindingKey key = new AclBindingKey(resourcePattern, scope);
      KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(key);

      partitionWriter.update(key,
          existingValue -> {
            Set<AclRule> updatedRules = new HashSet<>();
            if (existingValue != null)
              updatedRules.addAll(((AclBindingValue) existingValue).aclRules());
            bindings.forEach(binding -> updatedRules.add(AclRule.from(binding)));
            log.debug("New Acl binding scope={} resourcePattern={} accessRules={}", scope,
                resourcePattern, updatedRules);
            return new AclBindingValue(updatedRules);
          }).thenApply(unused -> new AclCreateResult(null))
          .exceptionally(e -> new AclCreateResult(new ProviderFailedException(e)))
          .thenAccept(result -> bindings.forEach(b -> futures.get(b).toCompletableFuture().complete(result)));
    });
  }

  private void populateAclCreateFailure(Map<AclBinding, CompletionStage<AclCreateResult>> futures, Throwable t) {
    futures.values().stream()
        .map(CompletionStage::toCompletableFuture)
        .filter(future -> !future.isDone())
        .forEach(future -> future.complete(new AclCreateResult(toApiException(t))));
  }

  private ApiException toApiException(Throwable t) {
    if (t instanceof RejectedExecutionException)
      return new NotMasterWriterException("This node is currently not the master writer for Metadata Service."
          + " This could be a transient exception during writer election.");
    else if (t instanceof ApiException)
      return (ApiException) t;
    else
      return new ApiException(t);
  }

  @Override
  public CompletionStage<Collection<AclBinding>> deleteAcls(final Scope scope,
      final AclBindingFilter filter,
      final Predicate<ResourcePattern> resourceAccess) {
    return deleteAcls(scope, Collections.singletonList(filter), resourceAccess).get(filter)
        .thenApply(result -> {
          if (result.exception().isPresent()) {
            throw result.exception().get();
          } else {
            return result.aclBindingDeleteResults().stream()
                .map(AclDeleteResult.AclBindingDeleteResult::aclBinding)
                .collect(Collectors.toList());
          }
        });
  }

  @Override
  public Map<AclBindingFilter, CompletionStage<AclDeleteResult>> deleteAcls(Scope scope,
        List<AclBindingFilter> filters,
        Predicate<ResourcePattern> resourceAccess) {

    log.debug("deleteAclRules scope={} aclBindingFilters={}", scope, filters);
    ensureMasterWriter();
    validateScope(scope);

    Map<AclBindingFilter, CompletionStage<AclDeleteResult>> futures = filters.stream()
        .collect(Collectors.toMap(Function.identity(), b -> new CompletableFuture<>()));

    try {
      // Schedule deletes after pending updates have completed
      CompletableFuture[] readyFutures = new CompletableFuture[partitionWriters.size()];
      for (int i = 0; i < partitionWriters.size(); i++) {
        readyFutures[i] = partitionWriters.get(i).incrementalUpdateFuture();
      }
      CompletableFuture.allOf(readyFutures)
          .thenAcceptAsync(v -> deleteAcls(scope, filters, resourceAccess, futures), writeExecutor)
          .whenComplete((unused, exception) -> {
            if (exception != null)
              populateAclDeleteFailure(futures, exception);
          });
    } catch (Throwable t) {
      populateAclDeleteFailure(futures, t);
    }
    return futures;
  }

  private void populateAclDeleteFailure(Map<AclBindingFilter, CompletionStage<AclDeleteResult>> futures, Throwable t) {
    futures.values().stream()
        .map(CompletionStage::toCompletableFuture)
        .filter(future -> !future.isDone())
        .forEach(future -> future.complete(new AclDeleteResult(toApiException(t))));
  }

  private void deleteAcls(Scope scope,
                          List<AclBindingFilter> filters,
                          Predicate<ResourcePattern> resourceAccess,
                          Map<AclBindingFilter, CompletionStage<AclDeleteResult>> futures) {

    log.trace("Scheduling deleteAcls for filters {}", filters);

    Map<AclBindingFilter, Collection<DeletableAclBinding>> toDeleteBindings = filters.stream()
        .collect(Collectors.toMap(Function.identity(), unused -> new LinkedList<>()));
    Map<ResourcePattern, Collection<DeletableAclBinding>> toDeleteRules = new HashMap<>();
    Map<AclBinding, DeletableAclBinding> bindingRules = new HashMap<>();

    ensureMasterWriter();

    // Collect all the matching ACLs and delete using single update for each resource pattern
    toDeleteBindings.forEach((filter, deleteList) -> {
      validateAclFilter(filter);
      //If filter matches with at-most one rule
      if (filter.matchesAtMostOne()) {
        ResourcePattern resourcePattern = ResourcePattern.from(filter.patternFilter());
        AclRule accessRule = AclRule.from(filter.entryFilter());

        if (resourceAccess.test(resourcePattern)) {
          AclBinding aclBinding = new AclBinding(ResourcePattern.to(resourcePattern),
                accessRule.toAccessControlEntry());
          DeletableAclBinding deletableBinding = new DeletableAclBinding(aclBinding, resourcePattern, accessRule);
          if (!bindingRules.containsKey(aclBinding)) {
            toDeleteRules.computeIfAbsent(resourcePattern, unused -> new LinkedList<>())
                .add(deletableBinding);
            deleteList.add(deletableBinding);
            bindingRules.put(aclBinding, deletableBinding);
          }
        }
      } else { // If filter matches with more than one rule
        Collection<AclBinding> matchedBindings = aclBindings(scope, filter, resourceAccess);
        matchedBindings.removeAll(bindingRules.keySet());

        for (AclBinding aclBinding : matchedBindings) {
          ResourcePattern resourcePattern = ResourcePattern.from(aclBinding.pattern());
          AclRule accessRule = AclRule.from(aclBinding);
          DeletableAclBinding deletableBinding = new DeletableAclBinding(aclBinding, resourcePattern, accessRule);
          toDeleteRules.computeIfAbsent(resourcePattern, v -> new LinkedList<>()).add(deletableBinding);
          deleteList.add(deletableBinding);
          bindingRules.put(aclBinding, deletableBinding);
        }
      }
    });

    toDeleteRules.forEach((resourcePattern, deletableBindings) -> {
      CompletionStage<Void> future = deleteAclRules(scope, resourcePattern,
          deletableBindings.stream().map(b -> b.aclRule).collect(Collectors.toList()));
      deletableBindings.forEach(b -> b.future = future.toCompletableFuture());
    });
    toDeleteBindings.forEach((filter, deleteFutures) -> {
          future(deleteFutures).thenAccept(result -> futures.get(filter).toCompletableFuture().complete(result));
    });
  }

  private CompletionStage<AclDeleteResult> future(Collection<DeletableAclBinding> bindings) {
    return CompletableFuture.allOf(bindings.stream().map(b -> b.future).collect(Collectors.toList())
        .toArray(new CompletableFuture[bindings.size()]))
        .thenApply(unused -> new AclDeleteResult(bindings.stream().map(DeletableAclBinding::deleteResult).collect(Collectors.toList())));
  }

  private CompletionStage<Void> deleteAclRules(final Scope scope,
                                               final ResourcePattern resourcePattern,
                                               final Collection<AclRule> deletedRules) {
    AclBindingKey key = new AclBindingKey(resourcePattern, scope);
    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(key);

    log.trace("deleteAclRules {} {}", resourcePattern, deletedRules);
    return partitionWriter.update(key,
        existingValue -> {
          Set<AclRule> updatedRules = new HashSet<>();
          if (existingValue != null)
            updatedRules.addAll(((AclBindingValue) existingValue).aclRules());
          updatedRules.removeAll(deletedRules);
          if (!updatedRules.isEmpty()) {
            log.debug("New Acl binding scope={} resourcePattern={} accessRules={}", scope, resourcePattern, updatedRules);
            return new AclBindingValue(updatedRules);
          } else {
            log.debug("Deleting Acl binding with scope={} resourcePattern={}", scope, resourcePattern);
            return null;
          }
        });
  }

  // Returns the current ACL bindings from the cache. The caller should wait for cache
  // to be up-to-date before invoking this method.
  private Collection<AclBinding> aclBindings(final Scope scope,
                                             final AclBindingFilter aclBindingFilter,
                                             final Predicate<ResourcePattern> resourceAccess) {
    log.debug("aclBindings scope={} aclBindingFilter={}", scope, aclBindingFilter);
    Set<AclBinding> aclBindings = new HashSet<>();
    Scope nextScope = scope;

    while (nextScope != null) {
      Map<ResourcePattern, Set<AccessRule>> rules = authCache.aclRules(nextScope);
      if (rules != null) {
        rules.entrySet().stream()
            .filter(e -> resourceAccess.test(e.getKey()))
            .forEach(e -> {
              org.apache.kafka.common.resource.ResourcePattern resourcePattern = ResourcePattern.to(e.getKey());
              e.getValue().forEach(accessRule -> {
                AclBinding fixture = new AclBinding(resourcePattern, AclRule.accessControlEntry(accessRule));
                if (aclBindingFilter.matches(fixture))
                  aclBindings.add(fixture);
              });
            });
      }
      nextScope = nextScope.parent();
    }
    return aclBindings;
  }

  private void ensureMasterWriter() {
    if (!isMasterWriter.get() || !ready)
      throw new NotMasterWriterException("This node is currently not the master writer for Metadata Service."
              + " This could be a transient exception during writer election.");
  }

  private void validateScope(final Scope scope) {
    scope.validate(true);
    if (!authCache.rootScope().containsScope(scope)) {
      throw new InvalidScopeException("This writer does not contain binding scope " + scope);
    }
  }

  private void validateAclBinding(final AclBinding aclBinding) {
    if (aclBinding.toFilter().findIndefiniteField() != null) {
      throw new InvalidRequestException("Invalid ACL creation: " + aclBinding);
    }

    if (aclBinding.pattern().resourceType().equals(ResourceType.CLUSTER)
        && !isClusterResource(aclBinding.pattern().name())) {
      throw new InvalidRequestException("The only valid name for the CLUSTER resource is "
          + Resource.CLUSTER_NAME);
    }
  }

  private void validateAclFilter(AclBindingFilter filter) {
    if (filter.isUnknown()) {
      throw new InvalidRequestException("The AclBindingFilter "
          + "must not contain UNKNOWN elements.");
    }
  }

  private boolean isClusterResource(final String name) {
    return name.equals(Resource.CLUSTER_NAME);
  }

  public void close(Duration closeTimeout) {
    if (alive.getAndSet(false)) {
      stopWriter(null);
      producer.close(closeTimeout);
    }
  }

  @Override
  public void onConsumerRecord(ConsumerRecord<AuthKey, AuthValue> record, AuthValue oldValue) {
    // If writing is not enabled yet, we can ignore the record.
    if (partitionWriters.isEmpty() || !partitionWriters.containsKey(record.partition()))
      return;

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(record.partition());
    AuthEntryType entryType = record.key().entryType();

    if (entryType == AuthEntryType.STATUS) {
      StatusValue statusValue = (StatusValue)  record.value();
      partitionWriter.onStatusConsumed(record.offset(), statusValue.generationId(), statusValue.status());
    } else {
      // If value hasn't changed, then it could be a duplicate whose write entry was
      // already cancelled and removed.
      boolean expectPendingWrite = !Objects.equals(record.value(), oldValue);
      partitionWriter.onRecordConsumed(record, oldValue, expectPendingWrite);
    }
  }

  void rebalanceListener(MetadataServiceRebalanceListener rebalanceListener) {
    if (this.rebalanceListener != null)
      throw new IllegalStateException("Rebalance listener already set on this writer");
    this.rebalanceListener = rebalanceListener;
  }

  /**
   * Writes an external metadata entry into the partition corresponding to the provided key.
   * External entries may be written to the topic before the partition is initialized
   * since initialization completes only after topic is populated with existing external
   * entries when the external store is first configured.
   *
   * @param key Key for new record
   * @param value Value for new record, may be null to delete the entry
   * @param expectedGenerationId Generation id currently associated with the external store
   */
  public void writeExternalEntry(AuthKey key, AuthValue value, int expectedGenerationId) {
    partitionWriter(partition(key)).write(key, value, expectedGenerationId, false, true);
  }

  public void writeExternalStatus(MetadataStoreStatus status, String errorMessage, int generationId) {
    ExecutorService executor = this.mgmtExecutor;
    if (executor != null && !executor.isShutdown()) {
      try {
        executor.submit(() -> {
          updateExternalStatus(status, errorMessage, generationId);
        });
      } catch (RejectedExecutionException e) {
        log.trace("Status could not be updated since executor has been shutdown");
        if (!executor.isShutdown())
          throw e;
      }
    }
  }

  private void updateExternalStatus(MetadataStoreStatus status, String errorMessage, int generationId) {
    try {
      boolean hasFailure = externalAuthStores.values().stream().anyMatch(ExternalStore::failed);
      switch (status) {
        case INITIALIZED:
          if (hasFailure)
            return;
          else
            break;
        case FAILED:
          if (!hasFailure)
            return;
          else
            break;
        default:
          throw new IllegalStateException("Unexpected status for external store " + status);
      }
      StatusValue statusValue = new StatusValue(status, generationId, config.brokerId, errorMessage);
      partitionWriters.forEach((partition, writer) ->
          writer.writeStatus(generationId, new StatusKey(partition), statusValue, status));
    } catch (Throwable e) {
      log.error("Failed to write external status to auth topic, writer resigning", e);
      rebalanceListener.onWriterResigned(generationId);
    }
  }

  private void createPartitionWriters() throws Throwable {
    maybeCreateAuthTopic(topic, config.topicCreateTimeout);
    if (numPartitions == 0)
      throw new IllegalStateException("Number of partitions not known for " + topic);
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      partitionWriters.put(i,
          new KafkaPartitionWriter<>(tp, producer, authCache, rebalanceListener, statusListener,
              config.refreshTimeout, time));
    }
  }

  private void maybeCreateAuthTopic(String topic, Duration topicCreateTimeout) {
    try (AdminClient adminClient = adminClientSupplier.get()) {
      KafkaUtils.waitForTopic(topic,
          numPartitions,
          time,
          topicCreateTimeout,
          t -> describeAuthTopic(t, adminClient),
          t -> createAuthTopic(adminClient, topic));
    }
  }

  private Set<Integer> describeAuthTopic(String topic, AdminClient adminClient) {
    try {
      if (!alive.get())
        throw new RuntimeException("KafkaAuthWriter has been shutdown");
      return adminClient.describeTopics(Collections.singleton(topic))
          .all().get().get(topic).partitions().stream()
          .map(TopicPartitionInfo::partition)
          .collect(Collectors.toSet());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof KafkaException)
        throw (KafkaException) cause;
      else
        throw new KafkaException("Failed to describe auth topic " + topic, cause);
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    }
  }

  private void createAuthTopic(AdminClient adminClient, String topic) {
    try {
      if (!alive.get())
        throw new RuntimeException("KafkaAuthWriter has been shutdown");
      NewTopic metadataTopic = config.metadataTopicCreateConfig(topic, numPartitions);
      log.info("Creating auth topic {}", metadataTopic);
      adminClient.createTopics(Collections.singletonList(metadataTopic)).all().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        log.debug("Topic was created by different node");
      } else {
        Throwable cause = e.getCause();
        if (cause instanceof KafkaException)
          throw (KafkaException) cause;
        else
          throw new KafkaException("Failed to create auth topic " + topic, cause);
      }
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    }
  }

  private AccessPolicy accessPolicy(String role) {
    Role roleDefinition = authCache.rbacRoles().role(role);
    if (roleDefinition == null)
      throw new InvalidRoleBindingException("Role not found " + role);
    else
      return roleDefinition.accessPolicy();
  }

  private void validateRoleBindingUpdate(String role, Scope scope, Collection<?> resources, boolean expectResourcesForResourceLevel) {
    ensureMasterWriter();
    validateScope(scope);
    AccessPolicy accessPolicy = accessPolicy(role);
    if (!resources.isEmpty() && !accessPolicy.hasResourceScope())
      throw new InvalidRequestException("Resources cannot be specified for role " + role +
          " with scope type " + accessPolicy.scopeType());
    else if (expectResourcesForResourceLevel && resources.isEmpty() && accessPolicy.hasResourceScope())
      throw new InvalidRequestException("Role binding update of resource-scope role without any resources");
  }

  private void validateRoleResources(Collection<ResourcePattern> resources) {
    resources.forEach(resource -> {
      if (resource.name() == null || resource.name().isEmpty())
        throw new InvalidRequestException("Resource name for role binding must be non-empty");
      if (resource.resourceType() == null || resource.resourceType().name() == null || resource.resourceType().name().isEmpty())
        throw new InvalidRequestException("Resource type for role binding must be non-empty");
      if (resource.patternType() == null || !resource.patternType().isSpecific())
        throw new InvalidRequestException("Resource pattern type for role binding must be LITERAL or PREFIXED, got " + resource);
    });
  }

  private int partition(AuthKey key) {
    return Utils.toPositive(key.hashCode()) % partitionWriters.size();
  }

  private KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter(int partition) {
    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriters.get(partition);
    if (partitionWriter == null)
      throw new IllegalArgumentException("Partition writer not found for partition " + partition);
    return partitionWriter;
  }

  private KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter(AuthKey key) {
    return partitionWriter(partition(key));
  }

  private void loadExternalAuthStores() {
    Map<String, ?> configs = config.originals();
    if (LdapConfig.ldapEnabled(configs)) {
      LdapStore ldapStore = createLdapStore(configs, authCache);
      externalAuthStores.put(AuthEntryType.USER, ldapStore);
    } else {
      externalAuthStores.put(AuthEntryType.USER, new DummyUserStore());
    }
  }

  // Visibility for testing
  protected LdapStore createLdapStore(Map<String, ?> configs, DefaultAuthCache authCache) {
    LdapStore ldapStore = new LdapStore(authCache, this, time);
    ldapStore.configure(configs);
    return ldapStore;
  }

  private class DummyUserStore implements ExternalStore {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void start(int generationId) {
      authCache.map(AuthEntryType.USER.name()).forEach((k, v) ->
          writeExternalEntry(k, null, generationId));
    }

    @Override
    public void stop(Integer generationId) {
    }

    @Override
    public boolean failed() {
      return false;
    }
  }

  private static class DeletableAclBinding {
    final AclBinding binding;
    final ResourcePattern resourcePattern;
    final AclRule aclRule;
    CompletableFuture<Void> future;
    DeletableAclBinding(AclBinding binding, ResourcePattern resourcePattern, AclRule aclRule) {
      this.binding = binding;
      this.resourcePattern = resourcePattern;
      this.aclRule = aclRule;
    }

    AclDeleteResult.AclBindingDeleteResult deleteResult() {
      try {
        return future.thenApply(v -> new AclDeleteResult.AclBindingDeleteResult(binding)).get();
      } catch (Throwable t) {
        return new AclDeleteResult.AclBindingDeleteResult(binding, new ProviderFailedException(t));
      }
    }

  }
}
