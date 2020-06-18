/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import io.confluent.databalancer.operation.BalanceOpExecutionCompletionCallback;

import java.util.Optional;
import java.util.Set;

/**
 * Interface to expose CruiseControl operations to users of that functionality (the KafkaDataBalanceManager). This is a limited subset of all the
 * interfaces that CruiseControl actually exposes, most of which aren't needed for the DataBalanceManager. The intent is that implementors
 * of this class will take care of managing synchronization and computation resources for CruiseControl and the methods exposed
 * here map 1:1 to the underlying CruiseControl operations.
 */
public interface DataBalanceEngine {

    /**
     * Return {@link DataBalanceEngineContext} associated with this DataBalanceEngine.
     */
    DataBalanceEngineContext getDataBalanceEngineContext();

    /**
     * To be called when this DataBalanceEngine should be activated and start running.
     */
    void onActivation(EngineInitializationContext initializationContext);

    /**
     * To be called when this DataBalanceEngine should stop execution. onActivation may be
     * called after this.
     */
    void onDeactivation();

    /**
     * To be called when the object is going away.
     */
    void shutdown() throws InterruptedException;

    /**
     * Update the replication throttles to be used during proposal execution.
     * @param newThrottle -- new throttle in bytes/second.
     */
    void updateThrottle(Long newThrottle);

    /**
     * Enable or disable auto-healing (automatic execution of rebalance plans) when an imbalanced
     * cluster is detected and broker membership doesn't change. Setting this to false DOES NOT
     * disable detection of newly-added (empty) brokers or user-initiated drain operations.
     *
     * @param shouldAutoHeal -- if auto-healing should be enabled when goal violations are detected.
     */
    void setAutoHealMode(boolean shouldAutoHeal);

    boolean isActive();

    /**
     * Schedules the removal of a broker
     */
    void removeBroker(int brokerToRemove,
                      Optional<Long> brokerToRemoveEpoch,
                      String uid);

    void addBrokers(Set<Integer> brokersToAdd, BalanceOpExecutionCompletionCallback onExecutionCompletion, String uid);

    /**
     * Cancel the on-going broker removal operation for the given #{@code brokerId}
     * @return - a boolean indicating whether the cancellation was successful
     */
    boolean cancelBrokerRemoval(int brokerId);
}
