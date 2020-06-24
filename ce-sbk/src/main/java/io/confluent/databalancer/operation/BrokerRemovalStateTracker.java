/*
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.databalancer.operation;

import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the nitty-gritty logic of tracking and advancing the broker removal state machine.
 *
 * #{@link BrokerRemovalStateTracker#initialize()} MUST be called on any state tracker instance before it can be utilized.
 */
@ThreadSafe
public class BrokerRemovalStateTracker implements BrokerRemovalCallback {

  private static final Logger LOG = LoggerFactory.getLogger(BrokerRemovalStateTracker.class);

  private final int brokerId;
  // store all the states that this state tracker has seen
  private Set<BrokerRemovalStateMachine.BrokerRemovalState> passedStates;
  private BrokerRemovalStateMachine stateMachine;
  private BrokerRemovalProgressListener progressListener;
  private BrokerRemovalTerminationListener terminationListener;
  private AtomicReference<String> stateReference;
  private volatile boolean cancelled = false;
  private volatile boolean initialized = false;

  /**
   * @param brokerId the ID of the broker that's going to be removed
   * @param progressListener the listener to call whenever the broker removal operation's progress changes
   * @param stateReference an atomic reference of the current state name to keep up to date
   */
  public BrokerRemovalStateTracker(int brokerId,
                                   BrokerRemovalProgressListener progressListener,
                                   BrokerRemovalTerminationListener terminationListener,
                                   AtomicReference<String> stateReference) {
    this(brokerId,
         new BrokerRemovalStateMachine(brokerId, BrokerRemovalStateMachine.START_STATE),
         progressListener,
         terminationListener,
         stateReference);
  }

  public BrokerRemovalStateTracker(int brokerId,
                                   BrokerRemovalStateMachine.BrokerRemovalState initState,
                                   BrokerRemovalProgressListener progressListener,
                                   BrokerRemovalTerminationListener terminationListener,
                                   AtomicReference<String> stateReference) {
    this(brokerId, new BrokerRemovalStateMachine(brokerId, initState), progressListener, terminationListener, stateReference);
  }

  // package-private for testing
  BrokerRemovalStateTracker(int brokerId, BrokerRemovalStateMachine stateMachine,
                            BrokerRemovalProgressListener progressListener,
                            BrokerRemovalTerminationListener terminationListener,
                            AtomicReference<String> stateReference) {
    this.brokerId = brokerId;
    this.stateMachine = stateMachine;
    this.progressListener = progressListener;
    this.terminationListener = terminationListener;
    this.stateReference = stateReference;
    this.passedStates = new HashSet<>();
    this.passedStates.add(stateMachine.currentState);
  }

  /**
   * Initialize the RemovalStateTracker by setting the current state and notifying the listener of the initial state.
   */
  public void initialize() {
    if (initialized) {
      throw new IllegalStateException("The state tracker was already initialized");
    }

    stateReference.set(stateMachine.currentState.name());

    if (stateMachine.currentState == BrokerRemovalStateMachine.START_STATE) {
      tryNotifyProgressChanged(stateMachine.currentState, null);
    }
    initialized = true;
  }

  public int brokerId() {
    return brokerId;
  }

  public BrokerRemovalStateMachine.BrokerRemovalState currentState() {
    return stateMachine.currentState;
  }

  @Override
  public synchronized void registerEvent(BrokerRemovalEvent pe) {
    registerEvent(pe, null);
  }

  @Override
  public synchronized void registerEvent(BrokerRemovalEvent pe, Exception eventException) {
    if (cancelled) {
      String errMsg = String.format("Will not register broker removal event %s (exception %s) for broker %d because the removal operation was already canceled.", pe, eventException, brokerId);
      LOG.warn(errMsg);
      throw new IllegalStateException(errMsg);
    }
    processEvent(pe, eventException);
  }

  /**
   * Cancels the broker removal state tracking by setting a terminal canceled state
   * @return a boolean indicating whether the operation was canceled
   */
  public synchronized boolean cancel(Exception eventException, BrokerRemovalCancellationMode cancellationMode) {
    if (canBeCanceled()) {
      if (cancellationMode == BrokerRemovalCancellationMode.PERSISTENT_CANCELLATION) {
        // broker restart is the only event that causes us to persist the cancellation right now
        processEvent(BrokerRemovalEvent.BROKER_RESTARTED,
            eventException);
      }
      cancelled = true;
    }

    return cancelled;
  }

  // package-private for testing
  boolean canBeCanceled() {
    return hasSeenState(BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_INITIATED)
        && !isDone();
  }

  /**
   * Return a boolean indicating whether this state tracker has gone through the given #{@code state}
   */
  boolean hasSeenState(BrokerRemovalStateMachine.BrokerRemovalState state) {
    return passedStates.contains(state);
  }

  /**
   * Returns a boolean indicating whether the current state is terminal.
   *
   * package-private for testing
   */
  private boolean isDone() {
    return stateMachine.currentState.isTerminal();
  }

  private void processEvent(BrokerRemovalEvent pe, Exception eventException) {
    if (!initialized) {
      throw new IllegalStateException("Cannot process a broker removal event because the state tracker is not initialized.");
    }
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

    passedStates.add(stateMachine.currentState);
    stateReference.set(newState.name());
    tryNotifyProgressChanged(newState, eventException);
  }

  private void tryNotifyProgressChanged(BrokerRemovalStateMachine.BrokerRemovalState state,
                                        Exception progressException) {
    try {
      this.progressListener.onProgressChanged(brokerId, state, progressException);
      LOG.debug("Notified progress listener of broker removal state change.");
    } catch (Exception e) {
      LOG.error("Error while notifying that broker removal operation progress changed for broker {}", brokerId);
    }
    if (state.isTerminal()) {
      try {
        this.terminationListener.onTerminalState(brokerId, state, progressException);
        LOG.debug("Notified progress listener of broker removal reaching terminal state.");
      } catch (Exception e) {
        LOG.error("Error while notifying that broker removal operation progress reached a terminal state {} for broker {}",
            state, brokerId);
      }
    }
  }
}
