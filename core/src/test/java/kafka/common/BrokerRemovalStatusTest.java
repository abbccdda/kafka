/*
 * Copyright (C) 2020 Confluent Inc.
 */
package kafka.common;

import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BrokerRemovalStatusTest {
  @Test
  public void testEquals() {
    BrokerRemovalStatus inProgressStatus = new BrokerRemovalStatus(
        1, BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
        BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS, null);
    BrokerRemovalStatus failedStatus = new BrokerRemovalStatus(
        1, BrokerRemovalDescription.BrokerShutdownStatus.FAILED,
        BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED, null);

    assertEquals(inProgressStatus, failedStatus);
    assertEquals(inProgressStatus.hashCode(), failedStatus.hashCode());
  }

  /**
   * Check if {@link BrokerRemovalStatus#isDone()} method returns correctly for all status values.
   */
  @Test
  public void testIsDone() {
    int brokerId = 1;

    // Go over all Shutdown status and Reassignments Status to validate isDone flag. This also makes
    // sure if we add a later enum value, we catch that case.
    for (BrokerRemovalDescription.PartitionReassignmentsStatus parStatus :
            BrokerRemovalDescription.PartitionReassignmentsStatus.values()) {
      for (BrokerRemovalDescription.BrokerShutdownStatus bssStatus :
              BrokerRemovalDescription.BrokerShutdownStatus.values()) {
        BrokerRemovalStatus removalStatus = new BrokerRemovalStatus(brokerId, bssStatus, parStatus, null);
        boolean expectedIsDone = parStatus == BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED ||
                parStatus == BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED ||
                parStatus == BrokerRemovalDescription.PartitionReassignmentsStatus.COMPLETE ||
                bssStatus == BrokerRemovalDescription.BrokerShutdownStatus.FAILED;

        Assert.assertEquals("Shutdown Status: " + bssStatus + ", Reassignment Status: " + parStatus,
                expectedIsDone, removalStatus.isDone());
      }
    }
  }
}
