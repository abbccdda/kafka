/*
 * Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.Confluent;

/**
 * This interface contains admin client methods that:
 * <ol>
 *     <li>are only present in Confluent Server, or</li>
 *     <li>existing Admin methods that return or take extra information only available in Confluent Server.</li>
 * </ol>
 *
 * Any new or update to admin client api that need these features should be done here.
 */
@Confluent
public interface ConfluentAdmin extends Admin {

    /**
     * Retrieves the status of the replicas for a set of partitions, including observers.
     *
     * <p>The status of the replicas will be as witnessed by the partition leader.  That is, the replicas
     * themselves may be further in progress than what's provided, however the leader has not yet processed
     * that state update.</p>
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code ReplicaStatusResult}:</p>
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.TopicAuthorizationException}
     *   If the authenticated user didn't have describe access to the Topic.</li>
     *   <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     *   If a given topic or partition does not exist.</li>
     *   <li>{@link org.apache.kafka.common.errors.NotLeaderForPartitionException}
     *   If the partition leader changed while the request was outstanding.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the controller could retrieve the partition's replica status.</li>
     * </ul>
     *
     * @param partitions The partitions to retrieve replica status for.
     * @param options The options to use.
     * @return The resulting replica status of every requested partition.
     */
    @Confluent
    ReplicaStatusResult replicaStatus(Set<TopicPartition> partitions, ReplicaStatusOptions options);

    /**
     * Creates access control lists (ACLs) which are bound to specific resources.
     * <p>
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     * <p>
     * If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but
     * no changes will be made.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param acls    The ACLs to create
     * @param options The options to use when creating the ACLs.
     * @param clusterId Cluster id for which ACLs are being updated
     * @param writerBrokerId Broker id of the current centralized metadata master writer
     * @return The CreateAclsResult.
     */
    @Confluent
    CreateAclsResult createCentralizedAcls(Collection<AclBinding> acls, CreateAclsOptions options, String clusterId, int writerBrokerId);

    /**
     * Deletes access control lists (ACLs) according to the supplied filters.
     * <p>
     * This operation is not transactional so it may succeed for some ACLs while fail for others.
     * <p>
     * This operation is supported by brokers with version 0.11.0.0 or higher.
     *
     * @param filters The filters to use.
     * @param options The options to use when deleting the ACLs.
     * @param clusterId Cluster id for which ACLs are being updated
     * @param writerBrokerId Broker id of the current centralized metadata master writer
     * @return The DeleteAclsResult.
     */
    @Confluent
    DeleteAclsResult deleteCentralizedAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options, String clusterId, int writerBrokerId);
}
