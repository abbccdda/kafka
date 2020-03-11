/*
 Copyright 2019 Confluent Inc.
 */

package io.confluent.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import java.util.List;


import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;

public class SequentialReplicaMovementGoal extends AbstractAcceptanceGoal {

    /**
     * Check if the action will move a replica of partition which already has a replica
     * movement planned, and reject it if it would. This is used to prevent parallel movements
     * of replicas of a partition.
     *
     * @param action Action to be checked for acceptance.
     * @param clusterModel State of the cluster before application of the action.
     * @return BROKER_REJECT if the action would move data between racks, otherwise ACCEPT
     */
    @Override
    public ActionAcceptance actionAcceptance(final BalancingAction action, final ClusterModel clusterModel) {
        switch (action.balancingAction()) {
            case LEADERSHIP_MOVEMENT:
            case INTRA_BROKER_REPLICA_MOVEMENT:
            case INTRA_BROKER_REPLICA_SWAP:
                return ACCEPT;
            case INTER_BROKER_REPLICA_MOVEMENT:
            case INTER_BROKER_REPLICA_SWAP:
                List<Replica> replicas = clusterModel.partition(action.topicPartition()).replicas();
                if (replicas.stream().anyMatch(Replica::isImmigrant)) {
                    return BROKER_REJECT;
                }

                return ACCEPT;
            default:
                throw new IllegalArgumentException("Balancing action " + action.balancingAction() +
                    "is not supported");
        }
    }

    @Override
    public String name() {
        return SequentialReplicaMovementGoal.class.getSimpleName();
    }
}
