/*
 * Copyright (C) 2020 Confluent Inc.
 */
package kafka.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BrokerAddStatusTest {

  @Test
  public void testEquals() {
    BrokerAddStatus firstStatus = new BrokerAddStatus(1, null);
    BrokerAddStatus secondStatus = new BrokerAddStatus(1, new RuntimeException("Test error"));

    assertEquals(firstStatus, secondStatus);
    assertEquals(firstStatus.hashCode(), secondStatus.hashCode());
  }
}
