/*
 * Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.Confluent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.requests.NewClusterLink;

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
     * Drain data off a given set of brokers and remove them from the cluster via shutdown.
     * This API initiates the removal. It is suggested that the user tracks it via {@link #describeBrokerRemovals()}
     *
     * Once initiated, the brokers will be shut down and replicas will be reassigned away from them.
     * <p>
     * This is a convenience method for {@link #removeBrokers(List, RemoveBrokersOptions)}
     * with default options. See the overload for more details.
     * </p>
     *
     * @param brokersToRemove   The broker IDs to drain off partition replicas and shut down. Must not be empty.
     * @return                  The result of the broker removal operation.
     */
    @Confluent
    default RemoveBrokersResult removeBrokers(List<Integer> brokersToRemove) {
        return removeBrokers(brokersToRemove, new RemoveBrokersOptions());
    }

    /**
     * Drain data off a given set of brokers and remove them from the cluster via shutdown.
     * This API initiates the removal. It is suggested that the user tracks it via {@link #describeBrokerRemovals()}
     *
     * Once initiated, the brokers will be shut down and replicas will be reassigned away from them.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@link RemoveBrokersResult}:</p>
     * <li> {@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     * If we didn't have sufficient permission to initiate the broker removal. None of the requests started. </li>
     * <li> {@link org.apache.kafka.common.errors.TimeoutException}
     * If the request timed out before the controller could initiate the broker removal.
     * It cannot be guaranteed whether the removal was initiated or not. </li>
     * <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     * If the broker removal request cannot be satisfied given the current cluster configuration.
     * For example, a negative broker id. </li>
     * <li> {@link org.apache.kafka.common.errors.BrokerNotAvailableException}
     * If broker ID doesn't exist, or is otherwise not available. </li>
     * <li> {@link org.apache.kafka.common.errors.BrokerRemovalInProgressException}
     * If the broker is already being removed. </li>
     * <li> {@link org.apache.kafka.common.errors.BrokerRemovedException}
     * If the broker was already removed successfully. </li>
     *
     * @param brokersToRemove  The broker IDs to drain off partition replicas and shut down. Must not be empty.
     * @param options          The options to use for the request.
     * @return                 The result of the broker removal operation.
     */
    @Confluent
    RemoveBrokersResult removeBrokers(List<Integer> brokersToRemove, RemoveBrokersOptions options);

    /**
     * Creates links to remote clusters with the specified configurations for performing inter-cluster
     * communications. Once established, the cluster links can referenced by their link names for issuing
     * requests.
     * <o>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code createClusterLinksResult}:
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.InvalidClusterLinkException}
     *   If the cluster link name is illegal.</li>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user didn't have {@code CREATE} access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.ClusterLinkExistsException}
     *   If a cluster link already exists for the provided link name.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the controller could create the cluster link.</li>
     * </ul>
     *
     * @param clusterLinks The cluster links to create.
     * @param options The options to use when creating the cluster links.
     * @return The CreateClusterLinksResult.
     */
    @Confluent
    CreateClusterLinksResult createClusterLinks(Collection<NewClusterLink> clusterLinks, CreateClusterLinksOptions options);

    /**
     * Lists the cluster links.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code listClusterLinksResult}:
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user didn't have {@code DESCRIBE_CONFIGS} access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the controller could list the cluster links.</li>
     * </ul>
     *
     * @param options The options to use when listing the cluster links.
     * @return The ListClusterLinksResult.
     */
    @Confluent
    ListClusterLinksResult listClusterLinks(ListClusterLinksOptions options);

    /**
     * Deletes established links to remote clusters with the provided link names.
     * <p>
     * Deleting a cluster link does not affect the cluster link's data in any way.
     * <p>
     * The following exceptions can be anticipated when calling {@code get()} on the futures obtained from
     * the returned {@code deleteClusterLinksResult}:
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.InvalidClusterLinkException}
     *   If the cluster link name is illegal.</li>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   If the authenticated user didn't have {@code DELETE} access to the cluster.</li>
     *   <li>{@link org.apache.kafka.common.errors.ClusterLinkNotFoundException}
     *   If the cluster link to delete doesn't exist.</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   If the request timed out before the controller could delete the cluster link.</li>
     * </ul>
     *
     * @param linkNames The names of the cluster links to delete.
     * @param options The options to use when deleting the cluster links.
     * @return The DeleteClusterLinksResult.
     */
    @Confluent
    DeleteClusterLinksResult deleteClusterLinks(Collection<String> linkNames, DeleteClusterLinksOptions options);
}
