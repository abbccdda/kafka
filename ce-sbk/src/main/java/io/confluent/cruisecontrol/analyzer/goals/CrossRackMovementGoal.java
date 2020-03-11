/*
 Copyright 2019 Confluent Inc.
 */

package io.confluent.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;

public class CrossRackMovementGoal extends AbstractAcceptanceGoal {

    /**
     * Check if the action will move data between racks, and reject it if it would. This is used to
     * prevent movement across availability zones in Confluent Cloud.
     *
     * @param action Action to be checked for acceptance.
     * @param clusterModel State of the cluster before application of the action.
     * @return BROKER_REJECT if the action would move data between racks, otherwise ACCEPT
     */
    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
        switch (action.balancingAction()) {
            case LEADERSHIP_MOVEMENT:
                return ACCEPT;
            case INTRA_BROKER_REPLICA_MOVEMENT:
            case INTER_BROKER_REPLICA_MOVEMENT:
            case INTER_BROKER_REPLICA_SWAP:
            case INTRA_BROKER_REPLICA_SWAP:
                if (!clusterModel.broker(action.sourceBrokerId()).rack().id().equals(
                        clusterModel.broker(action.destinationBrokerId()).rack().id())) {
                    return BROKER_REJECT;
                }

                return ACCEPT;
            default:
                throw new IllegalArgumentException("Balancing action " + action.balancingAction() +
                        "is not supported across racks");
        }
    }

    @Override
    public String name() {
        return CrossRackMovementGoal.class.getSimpleName();
    }
}
