/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.client;

import com.linkedin.kafka.cruisecontrol.BrokerShutdownIntegrationTestHarness;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitiateShutdownRequest;
import org.apache.kafka.common.requests.InitiateShutdownResponse;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(IntegrationTest.class)
public class BlockingSendClientIntegrationTest extends BrokerShutdownIntegrationTestHarness {

  @Override
  protected int numBrokers() {
    return 1;
  }

  @Test
  public void testSendShutdownRequest() throws Exception {
    long epoch = brokerToShutdownCurrentEpoch();
    InitiateShutdownResponse response = blockingSendClient().sendShutdownRequest(new InitiateShutdownRequest.Builder(epoch));

    assertEquals(Errors.NONE.code(), response.data().errorCode());
    assertBrokerShutdown();
  }

  @Test
  public void testSendShutdownRequestHitsError() throws Exception {
    long epoch = -111L; // errorful epoch
    InitiateShutdownResponse response = blockingSendClient().sendShutdownRequest(new InitiateShutdownRequest.Builder(epoch));

    assertEquals(Errors.STALE_BROKER_EPOCH.code(), response.data().errorCode());
    assertFalse(brokerExitCalled());
  }

  @Test(expected = IOException.class)
  public void testSendRequestOnOfflineBrokerShouldRaiseIOException() throws Exception {
    long epoch = brokerToShutdownCurrentEpoch();
    brokerToShutdown().shutdown();
    blockingSendClient().sendShutdownRequest(new InitiateShutdownRequest.Builder(epoch));
  }
}
