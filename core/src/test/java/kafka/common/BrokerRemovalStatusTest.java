/*
 * Copyright (C) 2020 Confluent Inc.
 */
package kafka.common;

import org.apache.kafka.clients.admin.BrokerRemovalDescription;
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
}
