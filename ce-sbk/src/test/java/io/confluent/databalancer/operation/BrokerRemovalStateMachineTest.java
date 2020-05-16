/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

import java.io.IOException;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.junit.Before;
import org.junit.Test;


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
import static org.junit.Assert.assertNull;


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
 *                      +-------------------+----------------+            +-----------+---------------+       +-------------+-----+--------+     +--------------------+---+-+              +--------------------------+
 *                                          |                                         |                                     |     |                                   |   |
 *                                   ERROR  |                                  ERROR  |                              ERROR  |     | BROKER                     ERROR  |   | BROKER
 *                                          |                                         |                                     |     | RESTART                           |   | RESTART
 *                                          |                                         |                                     |     |                                   |   |
 * +---------------------------------+      |    +------------------------+           |     +-------------------------+     |     |      +-----------------------+    |   |
 * | INITIAL_PLAN_COMPUTATION_FAILED |      |    | BROKER_SHUTDOWN_FAILED |           |     | PLAN_COMPUTATION_FAILED |     |     |      | PLAN_EXECUTION_FAILED |    |   |
 * |                                 |      |    |                        |           |     |                         |     |     |      |                       |    |   |
 * |  PAR=FAILED                     +<-----+    |   PAR=CANCELED         +<----------+     |   PAR=FAILED            <-----+     |      |   PAR=FAILED          +<---+   |
 * |  BSS=CANCELED                   |           |   BSS=FAILED           |                 |   BSS=COMPLETE          |           |      |   BSS=COMPLETE        |        |
 * +---------------------------------+           +------------------------+                 +-------------------------+           |      +-----------------------+        |
 *                                                                                                                                |                                       |
 *                                                                                                                                |                                       |
 *                                                                                          +-------------------------+           |      +------------------------+       |
 *                                                                                          |PLAN_COMPUTATION_CANCELED|           |      | PLAN_EXECUTION_CANCELED|       |
 *                                                                                          |                         |           |      |                        |       |
 *                                                                                          |   PAR=CANCELED          +<----------+      |    PAR=CANCELED        +<------+
 *                                                                                          |   BSS=COMPLETE          |                  |    BSS=COMPLETE        |
 *                                                                                          +-------------------------+                  +------------------------+
 * Created via https://asciiflow.com/
 */
public class BrokerRemovalStateMachineTest {

  private final int brokerId = 1;
  private BrokerRemovalStateMachine stateMachine;
  private Exception brokerShutdownException = new IOException("Error while shutting down broker!");
  private Exception planExecutionException = new ReassignmentInProgressException("Error while reassigning partitions!");
  private Exception planComputationFailure = new Exception("Plan computation failed!");

  @Before
  public void setUp() {
    stateMachine = new BrokerRemovalStateMachine(brokerId);
  }

  /**
   * Test the happy path of state transitions
   */
  @Test
  public void testAdvanceStateReachesCompletion() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);

    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);

    assertState(BROKER_SHUTDOWN_INITIATED);

    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS, null);

    assertState(PLAN_COMPUTATION_INITIATED);

    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS, null);

    assertState(PLAN_EXECUTION_INITIATED);

    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_SUCCESS, null);

    assertState(PLAN_EXECUTION_SUCCEEDED);
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
    // we cannot move from INITIAL_PLAN to PLAN_SUCCESS, it must be INITIAL_PLAN_SUCCESS/FAILURE
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS, null);
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidTerminalStateTransitionThrowsIllegalStateException() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE, null);
    assertState(INITIAL_PLAN_COMPUTATION_FAILED);
    // we cannot move from a terminal state to anything
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);
  }

  /**
   * Test state transition to when the plan execution is canceled (e.g restart of the broker being removed)
   */
  @Test
  public void testAdvanceStateTo_PlanExecutionCanceled() {
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS, null);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS, null);
    assertState(PLAN_EXECUTION_INITIATED);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_RESTARTED, null);
    assertState(PLAN_EXECUTION_CANCELED);
  }

  /**
   * Test state transition to when the plan execution is failed
   */
  @Test
  public void testAdvanceStateTo_PlanExecutionFailed() {
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS, null);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS, null);
    assertState(PLAN_EXECUTION_INITIATED);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE, planExecutionException);
    assertState(PLAN_EXECUTION_FAILED, planExecutionException);
  }

  /**
   * Test state transition to when the real plan computation is canceled (e.g due to broker restart)
   */
  @Test
  public void testAdvanceStateTo_PlanComputationCanceled() {
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS, null);
    assertState(PLAN_COMPUTATION_INITIATED);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_RESTARTED, null);
    assertState(PLAN_COMPUTATION_CANCELED);
  }

  /**
   * Test state transition to when the real plan computation fails
   */
  @Test
  public void testAdvanceStateTo_PlanComputationFail() {
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS, null);
    assertState(PLAN_COMPUTATION_INITIATED);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE, planComputationFailure);
    assertState(PLAN_COMPUTATION_FAILED, planComputationFailure);
  }

  /**
   * Test state transition to when the broker shutdown fails
   */
  @Test
  public void testAdvanceStateTo_ShutdownFailure() {
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);
    assertState(BROKER_SHUTDOWN_INITIATED);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE, brokerShutdownException);
    assertState(BROKER_SHUTDOWN_FAILED, brokerShutdownException);
  }

  /**
   * Test state transition to when the broker shutdown fails
   */
  @Test
  public void testAdvanceStateTo_InitialPlanFailure() {
    assertState(INITIAL_PLAN_COMPUTATION_INITIATED);
    stateMachine.advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE, planComputationFailure);
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
   * Asserts that the state machine is in the expected state/status for the given state
   * @param state - the state to assert
   */
  private void assertState(BrokerRemovalStateMachine.BrokerRemovalState state, Exception exception) {
    switch (state) {
      case INITIAL_PLAN_COMPUTATION_INITIATED:
        assertEquals(INITIAL_PLAN_COMPUTATION_INITIATED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.PENDING,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case INITIAL_PLAN_COMPUTATION_FAILED:
        assertEquals(INITIAL_PLAN_COMPUTATION_FAILED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.CANCELED,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case BROKER_SHUTDOWN_INITIATED:
        assertEquals(BROKER_SHUTDOWN_INITIATED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case BROKER_SHUTDOWN_FAILED:
        assertEquals(BROKER_SHUTDOWN_FAILED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.FAILED,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case PLAN_COMPUTATION_INITIATED:
        assertEquals(PLAN_COMPUTATION_INITIATED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case PLAN_COMPUTATION_FAILED:
        assertEquals(PLAN_COMPUTATION_FAILED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case PLAN_COMPUTATION_CANCELED:
        assertEquals(PLAN_COMPUTATION_CANCELED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case PLAN_EXECUTION_INITIATED:
        assertEquals(PLAN_EXECUTION_INITIATED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case PLAN_EXECUTION_CANCELED:
        assertEquals(PLAN_EXECUTION_CANCELED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case PLAN_EXECUTION_FAILED:
        assertEquals(PLAN_EXECUTION_FAILED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      case PLAN_EXECUTION_SUCCEEDED:
        assertEquals(PLAN_EXECUTION_SUCCEEDED, stateMachine.currentState);
        assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            stateMachine.brokerStatus().brokerShutdownStatus());
        assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.COMPLETE,
            stateMachine.brokerStatus().partitionReassignmentsStatus());
        break;
      default:
        throw new IllegalStateException("");
    }
    if (exception == null) {
      assertNull("Expected no exception to be populated",
          stateMachine.brokerStatus().exception());
    } else {
      assertEquals(exception, stateMachine.brokerStatus().exception());
    }
  }
}
