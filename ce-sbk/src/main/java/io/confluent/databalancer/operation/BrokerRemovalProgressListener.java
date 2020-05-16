/*
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.databalancer.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerRemovalProgressListener implements BrokerRemovalCallback {

  private static final Logger LOG = LoggerFactory.getLogger(BrokerRemovalProgressListener.class);

  private BrokerRemovalStateMachine stateMachine;

  public BrokerRemovalProgressListener(BrokerRemovalStateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  @Override
  public void registerEvent(BrokerRemovalEvent pe) {
    registerEvent(pe, null);
  }

  @Override
  public void registerEvent(BrokerRemovalEvent pe, Exception eventException) {
    try {
      stateMachine.advanceState(pe, eventException);
    } catch (Exception exception) {
      if (eventException != null) {
        LOG.error("Unexpected exception while handling removal event {} (event exception: {})!",
            pe, eventException, exception);
      } else {
        LOG.error("Unexpected exception while handling removal event {}!",
            pe, exception);
      }
    }
  }
}
