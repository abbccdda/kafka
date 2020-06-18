/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.clients.admin.BrokerRemovalDescription.BrokerShutdownStatus;
import org.apache.kafka.clients.admin.BrokerRemovalDescription.PartitionReassignmentsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.BROKER_RESTARTED;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_SUCCESS;

/**
 * This is an ASCII representation of the state machine diagram in
 * <a href="https://confluentinc.atlassian.net/wiki/spaces/CNKAF/pages/1219931556/SBK+Remove+Broker+Mega-Pager">
 *   https://confluentinc.atlassian.net/wiki/spaces/CNKAF/pages/1219931556/SBK+Remove+Broker+Mega-Pager</a>
 *
 *
 *                                                        SUCCESS                                      SUCCESS                          SUCCESS                             SUCCESS
 *                                       +-----------------------------------------------+ +--------------------------------+  +---------------------------+  +----------------------------------------+
 *                                       |                                               | |                                |  |                           |  |                                        |
 *                                       |                                               v |                                v  |                           v  |                                        v
 *                      +----------------+-------------------+            +--------------+-+----------+       +-------------+--+-----------+     +---------+--+-------------+              +-----------+--------------+
 *                      | INITIAL_PLAN_COMPUTATION_INITIATED |            | BROKER_SHUTDOWN_INITIATED |       | PLAN_COMPUTATION_INITIATED |     | PLAN_EXECUTION_INITIATED |              | PLAN_EXECUTION_SUCCEEDED |
 *                      |                                    |            |                           |       |                            |     |                          |              |                          |
 *                      | PAR=PENDING                        |            | PAR=IN_PROGRESS           |       | PAR=IN_PROGRESS            |     | PAR=IN_PROGRESS          |              | PAR=COMPLETE             |
 *                      | BSS=PENDING                        |            | BSS=PENDING               |       | BSS=COMPLETE               |     | BSS=COMPLETE             |              | BSS=COMPLETE             |
 *                      +-------------------+-+--------------+            +-----------+-+-------------+       +-------------+-----+--------+     +--------------------+---+-+              +--------------------------+
 *                                          |                                         | |                                   |     |                                   |   |
 *                                   ERROR  |                                  ERROR  | |                            ERROR  |     | BROKER                     ERROR  |   | BROKER
 *                                          |                                         | |                                   |     | RESTART                           |   | RESTART
 *                                          |                                         | |                                   |     |                                   |   |
 * +---------------------------------+      |    +------------------------+           | |   +-------------------------+     |     |      +-----------------------+    |   |
 * | INITIAL_PLAN_COMPUTATION_FAILED |      |    | BROKER_SHUTDOWN_FAILED |           | |   | PLAN_COMPUTATION_FAILED |     |     |      | PLAN_EXECUTION_FAILED |    |   |
 * |                                 |      |    |                        |           | |   |                         |     |     |      |                       |    |   |
 * |  PAR=FAILED                     +<-----+    |   PAR=CANCELED         +<----------+ |   |   PAR=FAILED            <-----+     |      |   PAR=FAILED          +<---+   |
 * |  BSS=CANCELED                   |           |   BSS=FAILED           |             |   |   BSS=COMPLETE          |           |      |   BSS=COMPLETE        |        |
 * +---------------------------------+           +------------------------+             |   +-------------------------+           |      +-----------------------+        |
 *                                                                                      |                                         |                                       |
 *                                                                                      |                                         |                                       |
 *                                               +------------------------+             |   +-------------------------+           |      +------------------------+       |
 *                                               |BROKER_SHUTDOWN_CANCELED|             |   |PLAN_COMPUTATION_CANCELED|           |      | PLAN_EXECUTION_CANCELED|       |
 *                                               |                        |             |   |                         |           |      |                        |       |
 *                                               |   PAR=CANCELED         +<------------+   |   PAR=CANCELED          +<----------+      |    PAR=CANCELED        +<------+
 *                                               |   BSS=CANCELED         |                 |   BSS=COMPLETE          |                  |    BSS=COMPLETE        |
 *                                               +------------------------+                 +-------------------------+                  +------------------------+
 * Created via https://asciiflow.com/
 */
@ThreadSafe
public class BrokerRemovalStateMachine {
  private static final Logger log = LoggerFactory.getLogger(BrokerRemovalStateMachine.class);

  static final BrokerRemovalState START_STATE = BrokerRemovalState.INITIAL_PLAN_COMPUTATION_INITIATED;

  private int brokerId;

  // package-private for testing
  BrokerRemovalState currentState;

  public BrokerRemovalStateMachine(int brokerId) {
    this(brokerId, START_STATE);
  }

  public BrokerRemovalStateMachine(int brokerId, BrokerRemovalState currentState) {
    this.brokerId = brokerId;
    this.currentState = currentState;
  }

  /**
   * React to a #{@link BrokerRemovalCallback.BrokerRemovalEvent}
   * by advancing the state machine.
   * @param event - the newly-occurred event on the broker removal operation
   * @throws IllegalStateException if the state transition is invalid
   * @return - the removal state after the advance
   */
  public synchronized BrokerRemovalState advanceState(BrokerRemovalCallback.BrokerRemovalEvent event) {
    if (currentState.isTerminal()) {
      throw new IllegalStateException(String.format("Cannot advance the state as %s is a terminal state", currentState.name()));
    }

    Optional<BrokerRemovalState> nextState = currentState.getNextState(event);
    if (!nextState.isPresent()) {
      throw new IllegalStateException(String.format("Cannot handle a %s removal event when in state %s", event, currentState.name()));
    }

    log.info("Broker removal state for broker {} transitioned from {} to {}.",
        brokerId, currentState, nextState.get());
    this.currentState = nextState.get();
    return currentState;
  }

  /**
   * All the valid removal states with links to valid state transitions
   */
  @Immutable
  public enum BrokerRemovalState {

    /**
     * The terminal state of when the initial plan computation fails. No further action is taken as part of the removal operation.
     */
    INITIAL_PLAN_COMPUTATION_FAILED(BrokerShutdownStatus.CANCELED, PartitionReassignmentsStatus.FAILED),

    /**
     * The terminal state of when the act of shutting down the broker fails. No further action is taken as part of the removal operation.
     */
    BROKER_SHUTDOWN_FAILED(BrokerShutdownStatus.FAILED, PartitionReassignmentsStatus.CANCELED),

    /**
     * The terminal state of when the real plan computation fails. No further action is taken as part of the removal operation.
     */
    PLAN_COMPUTATION_FAILED(BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.FAILED),

    /**
     * The terminal state of when the real plan computation is canceled. No further action is taken as part of the removal operation.
     */
    PLAN_COMPUTATION_CANCELED(BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.CANCELED),

    /**
     * The terminal state of when the final plan execution is canceled. No further action is taken as part of the removal operation.
     */
    PLAN_EXECUTION_CANCELED(BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.CANCELED),

    /**
     * The terminal state of when the final plan execution fails. No further action is taken as part of the removal operation.
     */
    PLAN_EXECUTION_FAILED(BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.FAILED),

    /**
     * The terminal state of when the final plan execution succeeds and the whole broker removal operation is finished.
     * No further action is taken.
     */
    PLAN_EXECUTION_SUCCEEDED(BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.COMPLETE),

    /**
     * The state after the plan computation succeeds and said plan execution is happening.
     */
    PLAN_EXECUTION_INITIATED(BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.IN_PROGRESS,
        new HashMap<BrokerRemovalCallback.BrokerRemovalEvent, BrokerRemovalState>() {{
          put(BROKER_RESTARTED, PLAN_EXECUTION_CANCELED);
          put(PLAN_EXECUTION_FAILURE, PLAN_EXECUTION_FAILED);
          put(PLAN_EXECUTION_SUCCESS, PLAN_EXECUTION_SUCCEEDED);
    }}),

    /**
     * The terminal state of when the broker shutdown is canceled. No further action is taken as part of the removal operation.
     */
    BROKER_SHUTDOWN_CANCELED(BrokerShutdownStatus.CANCELED, PartitionReassignmentsStatus.CANCELED),

    /**
     * The state after the broker shutdown succeeds and the real plan computation is happening.
     */
    PLAN_COMPUTATION_INITIATED(BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.IN_PROGRESS,
        new HashMap<BrokerRemovalCallback.BrokerRemovalEvent, BrokerRemovalState>() {{
          put(PLAN_COMPUTATION_FAILURE, PLAN_COMPUTATION_FAILED);
          put(BROKER_RESTARTED, PLAN_COMPUTATION_CANCELED);
          put(PLAN_COMPUTATION_SUCCESS, PLAN_EXECUTION_INITIATED);
    }}),

    /**
     * The state after the initial plan validation passes and SBK proceeds to shutting down the broker that is being removed.
     */
    BROKER_SHUTDOWN_INITIATED(BrokerShutdownStatus.PENDING, PartitionReassignmentsStatus.IN_PROGRESS,
        new HashMap<BrokerRemovalCallback.BrokerRemovalEvent, BrokerRemovalState>() {{
          put(BROKER_SHUTDOWN_FAILURE, BROKER_SHUTDOWN_FAILED);
          put(BROKER_SHUTDOWN_SUCCESS, PLAN_COMPUTATION_INITIATED);
          put(BROKER_RESTARTED, BROKER_SHUTDOWN_CANCELED);
    }}),

    /**
     * This is the initial state of broker removal, when the first plan (serving as validation pre-shutdown) is yet to be or is being computed.
     */
    INITIAL_PLAN_COMPUTATION_INITIATED(BrokerShutdownStatus.PENDING, PartitionReassignmentsStatus.PENDING,
        new HashMap<BrokerRemovalCallback.BrokerRemovalEvent, BrokerRemovalState>() {{
          put(INITIAL_PLAN_COMPUTATION_SUCCESS, BROKER_SHUTDOWN_INITIATED);
          put(INITIAL_PLAN_COMPUTATION_FAILURE, INITIAL_PLAN_COMPUTATION_FAILED);
        }});

    /**
     * Whether this state is terminal or not
     */
    private final boolean isTerminal;
    private final BrokerShutdownStatus shutdownStatus;
    private final PartitionReassignmentsStatus partitionReassignmentsStatus;
    private final Map<BrokerRemovalCallback.BrokerRemovalEvent, BrokerRemovalState> stateTransitions;

    /**
     * Create a terminal state with no available transitions
     */
    BrokerRemovalState(BrokerShutdownStatus shutdownStatus,
                       PartitionReassignmentsStatus partitionReassignmentsStatus) {
      this(shutdownStatus, partitionReassignmentsStatus, Collections.emptyMap());
    }

    BrokerRemovalState(BrokerShutdownStatus shutdownStatus,
                       PartitionReassignmentsStatus partitionReassignmentsStatus,
                       Map<BrokerRemovalCallback.BrokerRemovalEvent, BrokerRemovalState> stateTransitions) {
      this.shutdownStatus = shutdownStatus;
      this.partitionReassignmentsStatus = partitionReassignmentsStatus;
      this.stateTransitions = Collections.unmodifiableMap(stateTransitions);
      this.isTerminal = stateTransitions.isEmpty();
    }

    public Optional<BrokerRemovalState> getNextState(BrokerRemovalCallback.BrokerRemovalEvent event) {
      return Optional.ofNullable(stateTransitions.get(event));
    }

    /**
     * Denotes whether this is a terminal state
     */
    public boolean isTerminal() {
      return isTerminal;
    }

    /**
     * Returns the broker shutdown status for this state.
     */
    public BrokerShutdownStatus brokerShutdownStatus() {
      return shutdownStatus;
    }

    /**
     * Returns the partitions reassignment status for this state
     */
    public PartitionReassignmentsStatus partitionReassignmentsStatus() {
      return partitionReassignmentsStatus;
    }
  }
}
