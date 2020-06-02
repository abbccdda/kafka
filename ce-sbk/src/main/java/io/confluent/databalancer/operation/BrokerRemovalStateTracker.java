/*
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.databalancer.operation;

import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the nitty-gritty logic of tracking and advancing the broker removal state machine.
 */
public class BrokerRemovalStateTracker implements BrokerRemovalCallback {

  private static final Logger LOG = LoggerFactory.getLogger(BrokerRemovalStateTracker.class);

  private int brokerId;
  private BrokerRemovalStateMachine stateMachine;
  private BrokerRemovalProgressListener progressListener;
  private AtomicReference<String> stateReference;

  /**
   * Initialize the RemovalStateTracker by setting the current state and notifying the listener of the initial state.
   *
   * @param brokerId the ID of the broker that's going to be removed
   * @param progressListener the listener to call whenever the broker removal operation's progress changes
   * @param stateReference an atomic reference of the current state name to keep up to date
   */
  public static BrokerRemovalStateTracker initialize(int brokerId, BrokerRemovalProgressListener progressListener,
                                                     AtomicReference<String> stateReference) {
    return new BrokerRemovalStateTracker(brokerId, new BrokerRemovalStateMachine(brokerId), progressListener, stateReference);
  }

  // package-private for testing
  BrokerRemovalStateTracker(int brokerId, BrokerRemovalStateMachine stateMachine,
                            BrokerRemovalProgressListener progressListener, AtomicReference<String> stateReference) {
    this.brokerId = brokerId;
    this.stateMachine = stateMachine;
    this.progressListener = progressListener;
    this.stateReference = stateReference;
    stateReference.set(stateMachine.currentState.name());
    tryNotifyProgressChanged(stateMachine.currentState.brokerShutdownStatus(),
        stateMachine.currentState.partitionReassignmentsStatus(), null);
  }

  @Override
  public void registerEvent(BrokerRemovalEvent pe) {
    registerEvent(pe, null);
  }

  @Override
  public void registerEvent(BrokerRemovalEvent pe, Exception eventException) {
    BrokerRemovalStateMachine.BrokerRemovalState newState;

    try {
      newState = stateMachine.advanceState(pe);
    } catch (Exception exception) {
      if (eventException != null) {
        LOG.error("Unexpected exception while handling removal event {} (event exception: {})!",
            pe, eventException, exception);
      } else {
        LOG.error("Unexpected exception while handling removal event {}!",
            pe, exception);
      }
      throw exception;
    }

    stateReference.set(newState.name());
    tryNotifyProgressChanged(newState.brokerShutdownStatus(), newState.partitionReassignmentsStatus(),
        eventException);
  }

  private void tryNotifyProgressChanged(BrokerRemovalDescription.BrokerShutdownStatus shutdownStatus,
                                        BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus,
                                        Exception progressException) {
    try {
      this.progressListener.onProgressChanged(shutdownStatus, partitionReassignmentsStatus, progressException);
      LOG.debug("Notified progress listener of broker removal state change.");
    } catch (Exception e) {
      LOG.error("Error while notifying that broker removal operation progress changed for broker {}", brokerId);
    }
  }
}
