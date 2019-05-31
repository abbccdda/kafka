// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import io.confluent.security.auth.metadata.AuthWriter;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.provider.ldap.LdapStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthEntryType;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.external.ExternalStore;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.authorizer.utils.ThreadUtils;
import io.confluent.security.rbac.AccessPolicy;
import io.confluent.security.rbac.InvalidRoleBindingException;
import io.confluent.security.rbac.Role;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.CachedRecord;
import io.confluent.security.store.kafka.clients.ConsumerListener;
import io.confluent.security.store.kafka.clients.KafkaPartitionWriter;
import io.confluent.security.store.kafka.clients.Writer;
import io.confluent.security.store.kafka.coordinator.MetadataServiceRebalanceListener;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaAuthWriter implements Writer, AuthWriter, ConsumerListener<AuthKey, AuthValue> {

  private static final Logger log = LoggerFactory.getLogger(KafkaAuthWriter.class);

  private final String topic;
  private final KafkaStoreConfig config;
  private final Time time;
  private final DefaultAuthCache authCache;
  private final Producer<AuthKey, AuthValue> producer;
  private final Supplier<AdminClient> adminClientSupplier;
  private final Map<AuthEntryType, ExternalStore> externalAuthStores;
  private final AtomicBoolean isMasterWriter;
  private final Map<Integer, KafkaPartitionWriter<AuthKey, AuthValue>> partitionWriters;
  private MetadataServiceRebalanceListener rebalanceListener;
  private ExecutorService executor;
  private volatile boolean ready;

  public KafkaAuthWriter(String topic,
                         KafkaStoreConfig config,
                         Producer<AuthKey, AuthValue> producer,
                         Supplier<AdminClient> adminClientSupplier,
                         DefaultAuthCache authCache,
                         Time time) {
    this.topic = topic;
    this.config = config;
    this.producer = producer;
    this.adminClientSupplier = adminClientSupplier;
    this.authCache = authCache;
    this.time = time;
    this.externalAuthStores = new HashMap<>();
    this.isMasterWriter = new AtomicBoolean();
    this.partitionWriters = new HashMap<>();
    loadExternalAuthStores();
  }

  @Override
  public void startWriter(int generationId) {
    log.debug("Starting writer with generation id {}", generationId);
    if (generationId < 0)
      throw new IllegalArgumentException("Invalid generation id for master writer " + generationId);

    if (executor != null && !executor.isTerminated())
      throw new IllegalStateException("Starting writer without clearing startup executor of previous generation");
    executor = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("auth-writer-%d", true));

    executor.submit(() -> {
      try {
        if (partitionWriters.isEmpty()) {
          createPartitionWriters();
        }

        StatusValue initializing = new StatusValue(MetadataStoreStatus.INITIALIZING, generationId, null);
        partitionWriters.forEach((partition, writer) ->
            writer.start(generationId, new StatusKey(partition), initializing));
        ready = true;

      } catch (Throwable e) {
        log.error("Kafka auth writer initialization failed {}", e);
        rebalanceListener.onWriterResigned(generationId);
      }
    });

    executor.submit(() -> {
      try {
        externalAuthStores.forEach((type, store) -> store.start(generationId));
        writeExternalStatus(MetadataStoreStatus.INITIALIZED, null, generationId);
      } catch (Throwable e) {
        writeExternalStatus(MetadataStoreStatus.FAILED, e.getMessage(), generationId);
      }
    });

    isMasterWriter.set(true);
  }

  @Override
  public void stopWriter(Integer generationId) {
    try {
      ready = false;
      if (executor != null) {
        executor.shutdownNow();
        if (!executor.awaitTermination(config.refreshTimeout.toMillis(), TimeUnit.MILLISECONDS))
          throw new TimeoutException("Timed out waiting for start up to be terminated");
        executor = null;
      }
    } catch (InterruptedException e) {
      log.debug("Interrupted while shutting down writer executor");
      throw new InterruptException(e);
    } finally {
      externalAuthStores.values().forEach(store -> store.stop(generationId));
      partitionWriters.values().forEach(p -> p.stop(generationId));

      isMasterWriter.set(false);
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

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    CachedRecord<AuthKey, AuthValue> existingRecord =
        waitForExistingBinding(partitionWriter, principal, role, scope);
    Set<ResourcePattern> updatedResources = resources(existingRecord);
    updatedResources.addAll(newResources);

    log.debug("New binding {} {} {} {}", principal, role, scope, updatedResources);
    return partitionWriter.write(existingRecord.key(),
        new RoleBindingValue(updatedResources),
        existingRecord.generationIdDuringRead(),
        true);
  }

  @Override
  public CompletionStage<Void> replaceResourceRoleBinding(KafkaPrincipal principal,
                                                String role,
                                                Scope scope,
                                                Collection<ResourcePattern> resources) {
    log.debug("replaceResourceRoleBinding principal={} role={} scope={} resources={}", principal, role, scope, resources);
    validateRoleBindingUpdate(role, scope, resources, true);
    validateRoleResources(resources);

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);

    return partitionWriter.write(key, new RoleBindingValue(resources), null, true);
  }

  @Override
  public CompletionStage<Void> removeRoleBinding(KafkaPrincipal principal, String role, Scope scope) {
    log.debug("removeRoleBinding principal={} role={} scope={}", principal, role, scope);
    validateRoleBindingUpdate(role, scope, Collections.emptySet(), false);

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);

    return partitionWriter.write(key, null, null, true);
  }

  @Override
  public CompletionStage<Void> removeResourceRoleBinding(KafkaPrincipal principal,
                                                   String role,
                                                   Scope scope,
                                                   Collection<ResourcePatternFilter> deletedResources) {
    log.debug("removeResourceRoleBinding principal={} role={} scope={} resources={}", principal, role, scope, deletedResources);
    validateRoleBindingUpdate(role, scope, deletedResources, true);

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    CachedRecord<AuthKey, AuthValue> existingRecord =
        waitForExistingBinding(partitionWriter, principal, role, scope);
    Set<ResourcePattern> updatedResources = resources(existingRecord);
    deletedResources.forEach(pattern -> updatedResources.removeIf(pattern::matches));
    if (!updatedResources.isEmpty()) {
      RoleBindingValue value = new RoleBindingValue(updatedResources);

      log.debug("New binding {} {} {} {}", principal, role, scope, updatedResources);
      return partitionWriter.write(
          existingRecord.key(),
          value,
          existingRecord.generationIdDuringRead(),
          true);
    } else {
      log.debug("Deleting binding with no remaining resources {} {} {}", principal, role, scope);
      return partitionWriter.write(existingRecord.key(), null, null, true);
    }
  }

  public void close(Duration closeTimeout) {
    stopWriter(null);
    producer.close(closeTimeout);
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
    partitionWriter(partition(key)).write(key, value, expectedGenerationId, false);
  }

  public void writeExternalStatus(MetadataStoreStatus status, String errorMessage, int generationId) {
    ExecutorService executor = this.executor;
    if (executor != null) {
      executor.submit(() -> {
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
          StatusValue statusValue = new StatusValue(status, generationId, errorMessage);
          partitionWriters.forEach((partition, writer) ->
              writer.writeStatus(generationId, new StatusKey(partition), statusValue, status));
        } catch (Throwable e) {
          log.error("Failed to write external status to auth topic", e);
          rebalanceListener.onWriterResigned(generationId);
        }
      });
    }
  }

  private void createPartitionWriters() throws Throwable {
    int numPartitions = maybeCreateAuthTopic(topic);
    if (numPartitions == 0)
      throw new IllegalStateException("Number of partitions not known for " + topic);
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      partitionWriters.put(i,
          new KafkaPartitionWriter<>(tp, producer, authCache, rebalanceListener,
              config.refreshTimeout, time));
    }
  }

  private int maybeCreateAuthTopic(String topic) throws Throwable {
    int configuredPartitions = config.getInt(KafkaStoreConfig.NUM_PARTITIONS_PROP);
    try (AdminClient adminClient = adminClientSupplier.get()) {
      // Reader will timeout and fail broker shutdown, so it is ok to retry in a loop here
      while (true) {
        try {
          Set<Integer> partitions = adminClient.describeTopics(Collections.singleton(topic))
              .all().get().get(topic).partitions().stream()
              .map(TopicPartitionInfo::partition)
              .collect(Collectors.toSet());
          int numPartitions = partitions.size();
          if (numPartitions == configuredPartitions) {
            for (int i = 0; i < numPartitions; i++) {
              if (!partitions.contains(i)) {
                throw new IllegalStateException(String.format("Got %d partitions for %s as expected, but partition %d is missing",
                    configuredPartitions, topic, i));
              }
            }
            return numPartitions;
          } else if (numPartitions > configuredPartitions) {
            throw new IllegalStateException(String.format("Expected %d partitions for %s, got %d",
                configuredPartitions, topic, numPartitions));
          }
        } catch (ExecutionException e) {
          if (e.getCause() instanceof UnknownTopicOrPartitionException) {
            log.debug("Topic {} does not exist, creating new topic", topic);
            try {
              createAuthTopic(adminClient, topic);
            } catch (RetriableException | InvalidReplicationFactorException e1) {
              log.debug("Failed to create auth topic, retrying");
            }
          } else {
            log.error("Failed to describe auth topic", e);
            throw e;
          }
        }
      }
    }
  }

  private void createAuthTopic(AdminClient adminClient, String topic) throws Throwable {
    try {
      NewTopic metadataTopic = config.metadataTopicCreateConfig(topic);
      log.info("Creating metadata topic {}", metadataTopic);
      adminClient.createTopics(Collections.singletonList(metadataTopic)).all().get();
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof TopicExistsException)) {
        log.error("Failed to create auth topic", e.getCause());
        throw e.getCause();
      } else
        log.debug("Topic was created by different node");
    }
  }

  private CachedRecord<AuthKey, AuthValue> waitForExistingBinding(
      KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter,
      KafkaPrincipal principal,
      String role,
      Scope scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    return partitionWriter.waitForRefresh(key);
  }

  private AccessPolicy accessPolicy(String role) {
    Role roleDefinition = authCache.rbacRoles().role(role);
    if (roleDefinition == null)
      throw new InvalidRoleBindingException("Role not found " + role);
    else
      return roleDefinition.accessPolicy();
  }

  private void validateRoleBindingUpdate(String role, Scope scope, Collection<?> resources, boolean expectResourcesForResourceLevel) {
    if (!isMasterWriter.get() || !ready)
      throw new NotMasterWriterException("This node is currently not the master writer for Metadata Service."
          + " This could be a transient exception during writer election.");

    scope.validate(true);
    AccessPolicy accessPolicy = accessPolicy(role);
    if (!resources.isEmpty() && !accessPolicy.hasResourceScope())
      throw new IllegalArgumentException("Resources cannot be specified for role " + role +
          " with scope type " + accessPolicy.scopeType());
    else if (expectResourcesForResourceLevel && resources.isEmpty() && accessPolicy.hasResourceScope())
      throw new IllegalArgumentException("Role binding update of resource-scope role without any resources");

    if (!authCache.rootScope().containsScope(scope)) {
      throw new InvalidScopeException("This writer does not contain binding scope " + scope);
    }
  }

  private void validateRoleResources(Collection<ResourcePattern> resources) {
    resources.forEach(resource -> {
      if (resource.name() == null || resource.name().isEmpty())
        throw new IllegalArgumentException("Resource name for role binding must be non-empty");
      if (resource.resourceType() == null || resource.resourceType().name() == null || resource.resourceType().name().isEmpty())
        throw new IllegalArgumentException("Resource type for role binding must be non-empty");
      if (resource.patternType() == null || !resource.patternType().isSpecific())
        throw new IllegalArgumentException("Resource pattern type for role binding must be LITERAL or PREFIXED, got " + resource);
    });
  }

  private Set<ResourcePattern> resources(CachedRecord<AuthKey, AuthValue> record) {
    Set<ResourcePattern> resources = new HashSet<>();
    AuthValue value = record.value();
    if (value != null) {
      if (!(value instanceof RoleBindingValue))
        throw new IllegalArgumentException("Invalid record key=" + record.key() + ", value=" + value);
      resources.addAll(((RoleBindingValue) value).resources());
    }
    return resources;
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

  private KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter(KafkaPrincipal principal,
      String role,
      Scope scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    return partitionWriter(partition(key));
  }

  private void loadExternalAuthStores() {
    Map<String, ?> configs = config.originals();
    if (LdapConfig.ldapEnabled(configs)) {
      LdapStore ldapStore = new LdapStore(authCache, this, time);
      ldapStore.configure(configs);
      externalAuthStores.put(AuthEntryType.USER, ldapStore);
    } else {
      externalAuthStores.put(AuthEntryType.USER, new DummyUserStore());
    }
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
}