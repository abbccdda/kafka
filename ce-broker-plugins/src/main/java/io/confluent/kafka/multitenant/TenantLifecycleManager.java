package io.confluent.kafka.multitenant;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.kafka.multitenant.schema.TenantContext;
import io.confluent.kafka.multitenant.utils.Utils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TenantLifecycleManager {


    private static final Long CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    // We may try to list topics and ACLs before the broker even starts as we load the
    // metadata cache, so we don't want to wait too long if the broker isn't available
    private static final int LIST_METADATA_TIMEOUT_MS = 30 * 1000;
    private static final Logger LOG = LoggerFactory.getLogger(TenantLifecycleManager.class);
    private Long deleteDelayMs;
    private int deleteTopicBatchSize;
    private ExecutorService topicDeletionExecutor;
    private AdminClient adminClient;
    private AtomicBoolean adminClientCreated = new AtomicBoolean(false);

    enum State {
        ACTIVE,
        DEACTIVATED,
        DELETE_IN_PROGRESS,
        DELETED
    };

    final Map<String, State> tenantLifecycleState;

    public TenantLifecycleManager(Map<String, ?> configs) {

        this.tenantLifecycleState = new ConcurrentHashMap<>();

        Object deleteDelayValue =
                configs.get(ConfluentConfigs.MULTITENANT_TENANT_DELETE_DELAY_MS_CONFIG);
        if (deleteDelayValue == null)
            this.deleteDelayMs = ConfluentConfigs.MULTITENANT_TENANT_DELETE_DELAY_MS_DEFAULT;
        else
            this.deleteDelayMs = (long) deleteDelayValue;

        Object deleteTopicBatchSizeValue =
                configs.get(ConfluentConfigs.MULTITENANT_TENANT_DELETE_BATCH_SIZE_CONFIG);
        if (deleteTopicBatchSizeValue == null)
            this.deleteTopicBatchSize = ConfluentConfigs.MULTITENANT_TENANT_DELETE_BATCH_SIZE_DEFAULT;
        else
            this.deleteTopicBatchSize = (int) deleteTopicBatchSizeValue;
    }

    public void createAdminClient(String endpoint) {
        this.adminClient = Utils.createAdminClient(endpoint);
        // this shouldn't happen in real cluster, but we want to allow testing the cache without
        // a cluster.
        if (this.adminClient == null) {
            // NOTE: This error is important in production - it means we don't clean tenants properly
            // but test scenarios can set the admin client later and still pass
            LOG.error("We will mark clusters as deleted but will not be able to delete topics and ACLs because we " +
                    "failed to create admin client.");
        } else {
            adminClientCreated.compareAndSet(false, true);
        }
        this.topicDeletionExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                "tenant-topic-deletion-thread-%d").build());
    }

    // for unit tests
    TenantLifecycleManager(long deleteDelayMs, AdminClient adminClient) {
        this.tenantLifecycleState = new ConcurrentHashMap<>();
        this.deleteDelayMs = deleteDelayMs;
        this.adminClient = adminClient;
        if (this.adminClient != null)
            adminClientCreated.compareAndSet(false, true);
        this.deleteTopicBatchSize = 10;
        this.topicDeletionExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                "tenant-topic-deletion-thread-%d").build());
    }

    public void updateTenantState(LogicalClusterMetadata lcMeta) {
        String lcId = lcMeta.logicalClusterId();
        if (active(lcMeta)) {
            State prevState = tenantLifecycleState.put(lcId, State.ACTIVE);

            if (prevState == State.DEACTIVATED)
                LOG.info("Tenant {} was reactivated and will not be deleted", lcId);
            else if (prevState == State.DELETE_IN_PROGRESS || prevState == State.DELETED)
                LOG.warn("Attempted to reactive tenant {} but it was already deleted.", lcId);
        } else if (shouldDelete(lcMeta)) {
            tenantLifecycleState.put(lcId, State.DELETE_IN_PROGRESS);
        } else if (shouldDeactivate(lcMeta)) {
            tenantLifecycleState.put(lcId, State.DEACTIVATED);
            LOG.warn("Tenant {} was deactivated and will be deleted in {}.", lcId,
                    Duration.ofMillis(deleteDelayMs));
        }
    }

    /**
     * Return all logical clusters that are deactivated
     */
    public Set<String> inactiveClusters() {
        return tenantLifecycleState.entrySet().stream()
                .filter(lc -> lc.getValue() != State.ACTIVE)
                .map(lc -> lc.getKey())
                .collect(Collectors.toSet());
    }

    /**
     * Return all logical clusters that are still getting deleted
     * Used internally and for unit tests
     */
    Set<String> deleteInProgressClusters() {
        return tenantLifecycleState.entrySet().stream()
                .filter(lc -> lc.getValue() == State.DELETE_IN_PROGRESS)
                .map(lc -> lc.getKey())
                .collect(Collectors.toSet());
    }

    /**
     * Return all logical clusters that are considered deleted and we won't try to delete again
     * Used for testing
     */
    public Set<String> fullyDeletedClusters() {
        return tenantLifecycleState.entrySet().stream()
                .filter(lc -> lc.getValue() == State.DELETED)
                .map(lc -> lc.getKey())
                .collect(Collectors.toSet());
    }

    /**
     * Return all logical clusters that were deleted or being deleted
     * Used for testing
     * @return set of logical cluster IDs
     */
    public Set<String> deletedClusters() {
        return tenantLifecycleState.entrySet().stream()
                .filter(lc -> lc.getValue() == State.DELETE_IN_PROGRESS
                           || lc.getValue() == State.DELETED)
                .map(lc -> lc.getKey())
                .collect(Collectors.toSet());
    }

    // We check if the deactivated tenants have any topics or ACLs left.
    // If they do, we try to delete the topics and ACLs.
    // If the topics and ACLs are completely gone, we add them to the deletedClusters list and stop
    // deleting them.
    // Note that the retry logic here assumes that this method will be called on regular intervals.
    // Visible for testing
    public void deleteTenants() {
        if (!adminClientCreated.get())
            return;

        Set<String> deleteInProgressClusters = deleteInProgressClusters();
        if (deleteInProgressClusters.isEmpty())
            return;

        LOG.info("Deleting tenants in: {}", deleteInProgressClusters);

        Set<String> tenantsWithNoTopics = deleteTopics(deleteInProgressClusters);
        Set<String> tenantsWithNoACLs = deleteAcls(deleteInProgressClusters);

        // all the tenants with no topics *and* no ACLs are considered completely deleted
        for (String tenant: Sets.intersection(tenantsWithNoACLs, tenantsWithNoTopics))
            tenantLifecycleState.put(tenant, State.DELETED);
    }

    // Delete topics of deactivated tenants.
    // We do a bunch of setup here and then create a task and pass it to an executor because this
    // can take a while.
    // Note that the actual deletion is done synchronously, in small batches and one thread
    // This is to avoid taking over the controller for too long
    private Set<String> deleteTopics(Set<String> deleteInProgressClusters) {
        Set<String> tenantsWithNoTopics = new HashSet<>();
        List<String> topicsToDelete;

        try {
            ListTopicsOptions listTopicsOptions =
                    new ListTopicsOptions().timeoutMs(LIST_METADATA_TIMEOUT_MS);
            Set<String> topics = adminClient.listTopics(listTopicsOptions).names().get();
            topicsToDelete = topics.stream()
                    .filter(topic -> deleteInProgressClusters.contains(TenantContext.extractTenant(topic)))
                    .collect(Collectors.toList());
            Set<String> clustersWithTopicsToDelete =
                    topicsToDelete.stream().map(topic -> TenantContext.extractTenant(topic)).collect(Collectors.toSet());
            tenantsWithNoTopics =
                    Sets.difference(deleteInProgressClusters, clustersWithTopicsToDelete).immutableCopy();
            LOG.info("deleting topics {} because they belong to tenants {}", topicsToDelete, clustersWithTopicsToDelete);
        } catch (Exception e) {
            LOG.error("Failed to get list of topics in cluster.", e);
            return tenantsWithNoTopics; //can't delete if we have no topics...
        }

        Runnable deleteTopics = () -> {
            List<List<String>> topicBatches = Lists.partition(topicsToDelete, deleteTopicBatchSize);
            try {
                for (List<String> topicBatch : topicBatches) {
                    try {
                        adminClient.deleteTopics(topicBatch).all().get();
                        LOG.info("Successfully deleted topics {}", topicBatch);
                    } catch (UnknownTopicOrPartitionException e) {
                        // Do nothing and keep going. It just means the topic was already removed via another
                        // broker
                    }
                }
            } catch (InterruptedException e) {
                LOG.info("Deleting topics of deactivated tenants was interrupted");
            } catch (ExecutionException e) {
                LOG.error("Failed to delete topics for tenants {}. We'll try again next time",
                        deleteInProgressClusters, e);
            }
        };

        topicDeletionExecutor.execute(deleteTopics);

        return tenantsWithNoTopics;
    }

    private Set<String>  deleteAcls(Set<String> deleteInProgressClusters) {
        Set<String> tenantsWithNoACLs = new HashSet<>();
        Collection<AclBindingFilter> aclFiltersToDelete = new LinkedList<>();

        DescribeAclsOptions describeAclsOptions =
                new DescribeAclsOptions().timeoutMs(LIST_METADATA_TIMEOUT_MS);

        for (String lc: deleteInProgressClusters) {
            AclBindingFilter tenantFilter = new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.ANY, lc + TenantContext.DELIMITER,
                            PatternType.CONFLUENT_ALL_TENANT_ANY),
                    new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

            try {
                Collection<AclBinding> acls =
                        adminClient.describeAcls(tenantFilter, describeAclsOptions).values().get();
                if (acls.isEmpty())
                    tenantsWithNoACLs.add(lc);
                else
                    aclFiltersToDelete.add(tenantFilter);
            } catch (Exception e) {
                if (e.getCause() instanceof InvalidRequestException) {
                    LOG.error("Failed to get ACLs for tenants {} because this operation isn't supporting on "
                            + "this physical cluster. We won't retry and will consider deletion of ACLs "
                            + "for all tenants in list complete.", deleteInProgressClusters, e);
                    tenantsWithNoACLs.addAll(deleteInProgressClusters);
                    return tenantsWithNoACLs; // we know this cluster doesn't do ACLs, no point in trying all of them
                } else {
                    LOG.error("Failed to get ACLs for tenants {}. We'll try again next time",
                            deleteInProgressClusters, e);
                    return tenantsWithNoACLs;
                }
            }
        }

        adminClient.deleteAcls(aclFiltersToDelete);
        return tenantsWithNoACLs;
    }

    public void close() {
        if (topicDeletionExecutor != null)
            topicDeletionExecutor.shutdownNow();
        if (adminClient != null)
            adminClient.close(Duration.ofMillis(CLOSE_TIMEOUT_MS));
    }

    private boolean active(LogicalClusterMetadata lcMeta) {
        return lcMeta.lifecycleMetadata() == null ||
                lcMeta.lifecycleMetadata().deletionDate() == null;
    }

    private boolean shouldDeactivate(LogicalClusterMetadata lcMeta) {
        return (lcMeta.lifecycleMetadata() != null) &&
                (lcMeta.lifecycleMetadata().deletionDate() != null) &&
                lcMeta.lifecycleMetadata().deletionDate().before(new Date()) &&
                tenantLifecycleState.getOrDefault(lcMeta.logicalClusterId(), State.ACTIVE) == State.ACTIVE;
    }

    // topics are really deleted if the delay period has passed
    private boolean shouldDelete(LogicalClusterMetadata lcMeta) {
        Instant actualDeleteDate = Instant.now().minusMillis(deleteDelayMs);

        return (lcMeta.lifecycleMetadata() != null) &&
                (lcMeta.lifecycleMetadata().deletionDate() != null) &&
                lcMeta.lifecycleMetadata().deletionDate().before(Date.from(actualDeleteDate)) &&
                tenantLifecycleState.getOrDefault(lcMeta.logicalClusterId(), State.ACTIVE) != State.DELETED;
    }

    // Visibility for testing
    ExecutorService topicDeletionExecutor() {
        return topicDeletionExecutor;
    }

}
