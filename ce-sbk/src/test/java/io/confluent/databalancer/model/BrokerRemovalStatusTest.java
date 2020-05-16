/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.model;

import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BrokerRemovalStatusTest {
  @Test
  public void testEquals() {
    BrokerRemovalStatus inProgressStatus = new BrokerRemovalStatus(
        1, BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
        BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS, null);
    BrokerRemovalStatus failedStatus = new BrokerRemovalStatus(
        1, BrokerRemovalDescription.BrokerShutdownStatus.FAILED,
        BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED, null);

    assertNotEquals(inProgressStatus, failedStatus);
    BrokerRemovalStatus failedStatus2 = new BrokerRemovalStatus(
        1, BrokerRemovalDescription.BrokerShutdownStatus.FAILED,
        BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED, null);

    assertEquals(failedStatus2, failedStatus);
    assertEquals(failedStatus2.hashCode(), failedStatus.hashCode());
  }
}
