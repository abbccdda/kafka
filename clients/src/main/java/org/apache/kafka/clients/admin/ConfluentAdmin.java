/*
 * Copyright 2019 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * This interface contains admin client methods that:
 * <ol>
 *     <li>are only present in Confluent Server, or</li>
 *     <li>existing Admin methods that return or take extra information only available in Confluent Server.</li>
 * </ol>
 *
 * Any new or update to admin client api that need these features should be done here.
 */
public interface ConfluentAdmin extends Admin {

    /**
     * Create a new ConfluentAdmin with the given configuration.
     *
     * @param props The configuration.
     * @return A concrete class implementing ConfluentAdmin interface, normally KafkaAdminClient.
     */
    static ConfluentAdmin create(Properties props) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(props, true), null);
    }

    /**
     * Create a new ConfluentAdmin with the given configuration.
     *
     * @param conf The configuration.
     * @return A concrete class implementing ConfluentAdmin interface, normally KafkaAdminClient.
     */
    static ConfluentAdmin create(Map<String, Object> conf) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf, true), null);
    }

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
    ReplicaStatusResult replicaStatus(Set<TopicPartition> partitions, ReplicaStatusOptions options);
}
