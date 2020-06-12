/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A broker removal phase that can be part of Broker Removal state machine either when it
 * is started anew or if it restarted during failure recovery.
 */
public class BrokerRemovalRestartablePhase<T> implements BrokerRemovalPhase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerRemovalRestartablePhase.class);

    private final BrokerRemovalPhase<T> phase;
    private final boolean alwaysExecute;
    private final BrokerRemovalCallback brokerRemovalStateTracker;
    private boolean hasSkippedExecution;

    private BrokerRemovalRestartablePhase(BrokerRemovalCallback brokerRemovalStateTracker,
                                          BrokerRemovalPhase<T> phase,
                                          boolean alwaysExecute) {
        this.brokerRemovalStateTracker = brokerRemovalStateTracker;
        this.phase = phase;
        this.alwaysExecute = alwaysExecute;
    }

    @Override
    public T execute(BrokerRemovalOptions args) throws Exception {
        if (alwaysExecute || brokerRemovalStateTracker.currentState() == phase.startState()) {
            return phase.execute(args);
        } else {
            LOG.info("Skipping execution of {} as it doesn't match state machine state: {}",
                    phase.startState(), brokerRemovalStateTracker.currentState());
            hasSkippedExecution = true;
            return null;
        }
    }

    @Override
    public boolean hasSkippedExecution() {
        return hasSkippedExecution;
    }

    @Override
    public BrokerRemovalState startState() {
        return phase.startState();
    }

    public static class BrokerRemovalRestartablePhaseBuilder<T> {

        private BrokerRemovalPhase<T> phase;
        private boolean alwaysExecute = false;
        private BrokerRemovalCallback brokerRemovalStateTracker;

        public BrokerRemovalRestartablePhase<T> build() {
            return new BrokerRemovalRestartablePhase<T>(brokerRemovalStateTracker, phase, alwaysExecute);
        }

        public BrokerRemovalRestartablePhaseBuilder<T> setBrokerRemovalStateTracker(
                BrokerRemovalCallback brokerRemovalStateTracker) {
            this.brokerRemovalStateTracker = brokerRemovalStateTracker;
            return this;
        }

        public BrokerRemovalRestartablePhaseBuilder<T> setPhase(BrokerRemovalPhase<T> phase) {
            this.phase = phase;
            return this;
        }

        public BrokerRemovalRestartablePhaseBuilder<T> setAlwaysExecute(boolean alwaysExecute) {
            this.alwaysExecute = alwaysExecute;
            return this;
        }
    }
}
