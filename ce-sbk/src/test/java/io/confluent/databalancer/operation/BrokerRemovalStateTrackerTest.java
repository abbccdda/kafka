/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import kafka.common.BrokerRemovalStatus;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.common.errors.BrokerRemovalCanceledException;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.junit.Before;
import org.junit.Test;


import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_CANCELED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.INITIAL_PLAN_COMPUTATION_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.INITIAL_PLAN_COMPUTATION_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_CANCELED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_CANCELED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_SUCCEEDED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


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
 *
 * Tests that the #{@link BrokerRemovalStateTracker} and by extension #{@link BrokerRemovalStateMachine}
 * both do state transitions and notifications correctly.
 */
public class BrokerRemovalStateTrackerTest {

  private final int brokerId = 1;
  private BrokerRemovalStateMachine stateMachine;
  private BrokerRemovalStateTracker stateTracker;
  private Exception brokerShutdownException = new IOException("Error while shutting down broker!");
  private Exception planExecutionException = new ReassignmentInProgressException("Error while reassigning partitions!");
  private Exception planComputationFailure = new Exception("Plan computation failed!");
  private Exception canceledException = new BrokerRemovalCanceledException("Broker removal was canceled");
  private AtomicReference<String> stateRef = new AtomicReference<>("TEST");
  class TestListener implements BrokerRemovalProgressListener {
    private BrokerRemovalStatus status;
    private boolean terminalStateNotified = false;

    @Override
    public void onProgressChanged(BrokerRemovalDescription.BrokerShutdownStatus shutdownStatus, BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus, Exception e) {
      status = new BrokerRemovalStatus(brokerId, shutdownStatus, partitionReassignmentsStatus, e);
    }

    /**
     * Called when the state of the removal operation reaches a terminal point
     *
     * @param state - the terminal state
     * @param e     - nullable, the exception that caused the terminal state
     */
    @Override
    public void onTerminalState(BrokerRemovalStateMachine.BrokerRemovalState state, Exception e) {
      if (!state.isTerminal()) {
        throw new IllegalStateException("Got notified of a terminal state when we shouldn't have");
      }
      terminalStateNotified = true;
    }

    public BrokerRemovalStatus status() {
      return status;
    }
  }

  private TestListener listener = new TestListener();

  @Before
  public void setUp() {
    stateMachine = new BrokerRemovalStateMachine(brokerId);
    stateTracker = new BrokerRemovalStateTracker(brokerId, stateMachine, listener, stateRef);
    stateTracker.initialize();
  }

  @Test(expected = IllegalStateException.class)
  public void testAdvanceState_NotInitializedTracker_ThrowsException() {
    BrokerRemovalStateTracker stateTracker = new BrokerRemovalStateTracker(brokerId, stateMachine, listener, stateRef);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
  }

  /**
   * Should not be able to initialize the tracker twice
   */
  @Test(expected = IllegalStateException.class)
  public void testInitialize_InitializedTracker_ThrowsException() {
    BrokerRemovalStateTracker stateTracker = new BrokerRemovalStateTracker(brokerId, stateMachine, listener, stateRef);
    stateTracker.initialize();
    stateTracker.initialize();
  }

  /**
   * Test the happy path of state transitions
   */
  @Test
  public void testAdvanceStateReachesCompletion() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);

    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);

    assertState(BROKER_SHUTDOWN_INITIATED);
    assertTrue("Should be able to cancel state tracker as it has reached the shutdown state", stateTracker.canBeCanceled());

    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);

    assertState(PLAN_COMPUTATION_INITIATED);
    assertTrue("Should be able to cancel state tracker as it has reached the shutdown state", stateTracker.canBeCanceled());

    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);

    assertState(PLAN_EXECUTION_INITIATED);
    assertTrue("Should be able to cancel state tracker as it has reached the shutdown state", stateTracker.canBeCanceled());

    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_SUCCESS);

    assertState(PLAN_EXECUTION_SUCCEEDED);
    assertFalse("Should not be able to cancel state tracker as it has reached a terminal", stateTracker.canBeCanceled());

    assertStatesSeen(INITIAL_PLAN_COMPUTATION_INITIATED,
        BROKER_SHUTDOWN_INITIATED,
        PLAN_COMPUTATION_INITIATED,
        PLAN_EXECUTION_INITIATED,
        PLAN_EXECUTION_SUCCEEDED
    );
  }

  /**
   * A broker removal always starts with the initial plan computation
   */
  @Test
  public void testStateMachineStartsInInitialPlanComputationState() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidStateTransitionThrowsIllegalStateException() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);
    assertFalse("Should not be able to cancel state tracker as it has reached the shutdown state", stateTracker.canBeCanceled());

    // we cannot move from INITIAL_PLAN to PLAN_SUCCESS, it must be INITIAL_PLAN_SUCCESS/FAILURE
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidTerminalStateTransitionThrowsIllegalStateException() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE);
    assertState(INITIAL_PLAN_COMPUTATION_FAILED);
    // we cannot move from a terminal state to anything
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
  }

  /**
   * Test state transition to when the plan execution is canceled (e.g restart of the broker being removed)
   */
  @Test
  public void testAdvanceStateTo_PlanExecutionCanceled() {
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
    assertState(PLAN_EXECUTION_INITIATED);
    stateTracker.cancel(canceledException);
    assertState(PLAN_EXECUTION_CANCELED, canceledException);
    assertStatesNotSeen(BROKER_SHUTDOWN_CANCELED, BROKER_SHUTDOWN_FAILED, PLAN_COMPUTATION_FAILED, INITIAL_PLAN_COMPUTATION_FAILED);
    assertFalse("Should not be able to cancel state tracker as its in a terminal state", stateTracker.canBeCanceled());
  }

  /**
   * Test state transition to when the plan execution is canceled (e.g restart of the broker being removed)
   */
  @Test
  public void testCancel_FurtherEventRegistrationsAreNotProcessed() {
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);

    // act
    stateTracker.cancel(canceledException);
    assertState(BROKER_SHUTDOWN_CANCELED, canceledException);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE);

    // assert - nothing should have changed
    assertState(BROKER_SHUTDOWN_CANCELED, canceledException);
    assertStatesNotSeen(BROKER_SHUTDOWN_FAILED);
  }

  /**
   * Test state transition to when the plan execution is failed
   */
  @Test
  public void testAdvanceStateTo_PlanExecutionFailed() {
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS);
    assertState(PLAN_EXECUTION_INITIATED);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE, planExecutionException);
    assertState(PLAN_EXECUTION_FAILED, planExecutionException);

    assertStatesSeen(INITIAL_PLAN_COMPUTATION_INITIATED, BROKER_SHUTDOWN_INITIATED, PLAN_COMPUTATION_INITIATED, PLAN_EXECUTION_FAILED);
    assertStatesNotSeen(BROKER_SHUTDOWN_CANCELED, BROKER_SHUTDOWN_FAILED, PLAN_COMPUTATION_FAILED,
        INITIAL_PLAN_COMPUTATION_FAILED, PLAN_EXECUTION_SUCCEEDED, PLAN_EXECUTION_CANCELED);
  }

  /**
   * Test state transition to when the real plan computation is canceled (e.g due to broker restart)
   */
  @Test
  public void testAdvanceStateTo_PlanComputationCanceled() {
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
    assertState(PLAN_COMPUTATION_INITIATED);
    stateTracker.cancel(canceledException);
    assertState(PLAN_COMPUTATION_CANCELED, canceledException);
  }

  /**
   * Test state transition to when the real plan computation fails
   */
  @Test
  public void testAdvanceStateTo_PlanComputationFail() {
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);
    assertState(PLAN_COMPUTATION_INITIATED);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE, planComputationFailure);
    assertState(PLAN_COMPUTATION_FAILED, planComputationFailure);
  }

  /**
   * Test state transition to when the broker shutdown fails
   */
  @Test
  public void testAdvanceStateTo_ShutdownFailure() {
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
    assertState(BROKER_SHUTDOWN_INITIATED);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE, brokerShutdownException);
    assertState(BROKER_SHUTDOWN_FAILED, brokerShutdownException);
  }

  /**
   * Test state transition to when the broker shutdown is canceled
   */
  @Test
  public void testAdvanceStateTo_ShutdownCancellation() {
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
    assertState(BROKER_SHUTDOWN_INITIATED);
    stateTracker.cancel(canceledException);
    assertState(BROKER_SHUTDOWN_CANCELED, canceledException);
  }

  /**
   * Test state transition to when the initial plan computation fails
   */
  @Test
  public void testAdvanceStateTo_InitialPlanFailure() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);
    stateTracker.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE, planComputationFailure);
    assertState(INITIAL_PLAN_COMPUTATION_FAILED, planComputationFailure);
  }

  /**
   * Asserts that the state machine is in the expected state/status for the given state
   * @param state - the state to assert
   */
  private void assertState(BrokerRemovalStateMachine.BrokerRemovalState state) {
    assertState(state, null);
  }

  /**
   * Asserts that the state machine has notified us that it is in the expected state/status for the given state
   * @param state - the state to assert
   */
  private void assertState(BrokerRemovalStateMachine.BrokerRemovalState state, Exception exception) {
    switch (state) {
      case INITIAL_PLAN_COMPUTATION_INITIATED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.PENDING,
            listener.status().partitionReassignmentsStatus());
        break;
      case INITIAL_PLAN_COMPUTATION_FAILED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.CANCELED,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            listener.status().partitionReassignmentsStatus());
        break;
      case BROKER_SHUTDOWN_INITIATED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
            listener.status().partitionReassignmentsStatus());
        break;
      case BROKER_SHUTDOWN_FAILED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.FAILED,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED,
            listener.status().partitionReassignmentsStatus());
        break;
      case PLAN_COMPUTATION_INITIATED:
      case PLAN_EXECUTION_INITIATED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
            listener.status().partitionReassignmentsStatus());
        break;
      case PLAN_COMPUTATION_FAILED:
      case PLAN_EXECUTION_FAILED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            listener.status().partitionReassignmentsStatus());
        break;
      case PLAN_COMPUTATION_CANCELED:
      case PLAN_EXECUTION_CANCELED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED,
            listener.status().partitionReassignmentsStatus());
        break;
      case BROKER_SHUTDOWN_CANCELED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.CANCELED,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED,
            listener.status().partitionReassignmentsStatus());
        break;
      case PLAN_EXECUTION_SUCCEEDED:
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            listener.status().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.COMPLETE,
            listener.status().partitionReassignmentsStatus());
        break;
      default:
        throw new IllegalStateException("");
    }

    assertEquals(state, stateMachine.currentState);
    assertStatesSeen(state);
    assertEquals(state.name(), stateRef.get());
    assertEquals(state.name(), stateTracker.currentState());
    if (exception == null) {
      assertNull("Expected no exception to be populated",
          listener.status().exception());
    } else {
      assertEquals(exception, listener.status().exception());
    }
    if (state.isTerminal()) {
      assertTrue("Expected listener to be notified of the terminal state",
          listener.terminalStateNotified);
    }
  }

  private void assertStatesSeen(BrokerRemovalStateMachine.BrokerRemovalState... states) {
    for (BrokerRemovalStateMachine.BrokerRemovalState state : states) {
      assertTrue("Expected state tracker to have gone through the given state",
          stateTracker.hasSeenState(state));
    }
  }

  private void assertStatesNotSeen(BrokerRemovalStateMachine.BrokerRemovalState... states) {
    for (BrokerRemovalStateMachine.BrokerRemovalState state : states) {
      assertFalse("Expected state tracker to NOT have gone through the given state",
          stateTracker.hasSeenState(state));
    }
  }
}
