/*
 * Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
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

    /**
     * Drain data off a given set of brokers. Once initiated, the brokers will not have any partitions placed
     * on them.
     * <p>
     * This is a convenience method for {@link #drainBrokers(List, DrainBrokersOptions)}
     * with default options. See the overload for more details.
     * </p>
     *
     * @param brokersToDrain    The broker IDs to drain off partition replicas. Must not be empty.
     * @return                  The result of the drain operation.
     */
    @Confluent
    default DrainBrokersResult drainBrokers(List<Integer> brokersToDrain) {
        return drainBrokers(brokersToDrain, new DrainBrokersOptions());
    }

    /**
     * Drain data off a given set of brokers. Once initiated, the brokers will not have any partitions placed on
     * them and all the partition replicas on the broker will be reassigned to other active brokers in the
     * cluster.
     * A broker ID that is drained will not have data placed on it until it is gracefully restarted.
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code DrainBrokersResult}:</p>
     * <li> {@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     * If we didn't have sufficient permission to initiate the drain. None of the requests started. </li>
     * <li> {@link org.apache.kafka.common.errors.TimeoutException}
     * If the request timed out before the controller could initiate the drain. It cannot be guaranteed whether the
     * drain initiated or not. </li>
     * <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     * If drain request cannot be satisfied given the current cluster configuration. For example, a negative broker
     * id. </li>
     * <li> {@link org.apache.kafka.common.errors.BrokerNotAvailableException}
     * If broker ID doesn't exist, or is otherwise not available. </li>
     * <li> {@link org.apache.kafka.common.errors.ReassignmentInProgressException}
     * If a drain request is already in progress on the broker. </li>
     *
     * @param brokersToDrain   The broker IDs to drain off partition replicas. Must not be empty.
     * @param options          The options to use for the request.
     * @return                 The result of the drain operation.
     */
    @Confluent
    DrainBrokersResult drainBrokers(List<Integer> brokersToDrain, DrainBrokersOptions options);
}
