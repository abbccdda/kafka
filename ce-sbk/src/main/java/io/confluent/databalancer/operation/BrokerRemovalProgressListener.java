package io.confluent.databalancer.operation;

import org.apache.kafka.clients.admin.BrokerRemovalDescription;

/**
 * A listener for the progress of a broker removal operation
 */
public interface BrokerRemovalProgressListener {
  /**
   * Called whenever the state of the removal operation changes.
   * @param shutdownStatus the new broker shutdown status of the operation
   * @param partitionReassignmentsStatus the new partition reassignment shutdown status of the operation
   * @param e - nullable, an exception that occurred during the broker removal op
   */
  void onProgressChanged(BrokerRemovalDescription.BrokerShutdownStatus shutdownStatus,
                         BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus,
                         Exception e);

  /**
   * Called when the state of the removal operation reaches a terminal point
   *
   * @param state - the terminal state
   * @param e - nullable, the exception that caused the terminal state
   */
  void onTerminalState(BrokerRemovalStateMachine.BrokerRemovalState state, Exception e);
}
