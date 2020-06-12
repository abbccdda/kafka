/*
 * Copyright (C) 2020 Confluent Inc.
 */
package kafka.common;

import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BrokerRemovalDescriptionInternalTest {
  @Test
  public void testEquals() {
    BrokerRemovalDescriptionInternal inProgressStatus = new BrokerRemovalDescriptionInternal(
        1, BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
        BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS, null);
    BrokerRemovalDescriptionInternal failedStatus = new BrokerRemovalDescriptionInternal(
        1, BrokerRemovalDescription.BrokerShutdownStatus.FAILED,
        BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED, null);

    assertEquals(inProgressStatus, failedStatus);
    assertEquals(inProgressStatus.hashCode(), failedStatus.hashCode());
  }
}
