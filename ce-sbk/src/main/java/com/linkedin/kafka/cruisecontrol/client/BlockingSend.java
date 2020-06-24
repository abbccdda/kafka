/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.client;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.IOException;

public interface BlockingSend {
  ClientResponse sendRequest(AbstractRequest.Builder<? extends AbstractRequest> requestBuilder) throws IOException;

  void initiateClose();

  void close() throws Exception;
}
