/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.client;

import kafka.cluster.BrokerEndPoint;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.InitiateShutdownRequest;
import org.apache.kafka.common.requests.InitiateShutdownResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * #{@link BlockingSendClient} is a client used for sending internal inter-broker requests.
 * It is blocking.
 * This class is not thread-safe! It is very similar to #{@link kafka.server.ReplicaFetcherBlockingSend}.
 */
@NotThreadSafe
public class BlockingSendClient implements BlockingSend {

  private final KafkaConfig config;
  private final int socketTimeout;
  private final Time time;
  private final KafkaClient networkClient;
  private final Optional<? extends Reconfigurable> reconfigurableChannelBuilder;
  private final Node targetNode;

  public static class Builder {

    private final static String METRIC_GROUP_PREFIX = "sbk-internal-broker-client";
    private final KafkaConfig config;
    private final Metrics metrics;
    private final Time time;
    private final String clientId;
    private final LogContext logContext;

    public Builder(KafkaConfig config, Metrics metrics, Time time, String clientId, LogContext logContext) {
      this.config = config;
      this.metrics = metrics;
      this.time = time;
      this.clientId = clientId;
      this.logContext = logContext;
    }

    public BlockingSendClient build(BrokerEndPoint targetBroker) {
      ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol(), JaasContext.Type.SERVER, config,
          config.interBrokerListenerName(), config.saslMechanismInterBrokerProtocol(),
          time, config.saslInterBrokerHandshakeRequestEnable(), logContext
      );
      Optional<? extends Reconfigurable> reconfigurableChannelBuilder;
      if (channelBuilder instanceof Reconfigurable) {
        Reconfigurable reconfigurableBuilder = (Reconfigurable) channelBuilder;
        config.addReconfigurable(reconfigurableBuilder);
        reconfigurableChannelBuilder = Optional.of(reconfigurableBuilder);
      } else {
        reconfigurableChannelBuilder = Optional.empty();
      }
      Map<String, String> metricTags = new HashMap<>();
      metricTags.put("broker-id", Integer.toString(targetBroker.id()));

      Selector selector = new Selector(NetworkReceive.UNLIMITED, config.connectionsMaxIdleMs(),
          metrics, time, METRIC_GROUP_PREFIX, metricTags, true, channelBuilder, logContext);
      int maxInFlightRequestsPerConnection = 1;
      KafkaClient networkClient = new NetworkClient(selector, new ManualMetadataUpdater(), clientId, maxInFlightRequestsPerConnection,
          0, 0, Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.socketReceiveBufferBytes(), config.requestTimeoutMs(),
          ClientDnsLookup.DEFAULT, time, false, new ApiVersions(), logContext);

      Node targetNode = new Node(targetBroker.id(), targetBroker.host(), targetBroker.port());
      return new BlockingSendClient(targetNode, config, config.controllerSocketTimeoutMs(), time, networkClient, reconfigurableChannelBuilder);
    }
  }

  // package-private for testing
  BlockingSendClient(Node targetBroker, KafkaConfig config,
                     int socketTimeout, Time time,
                     KafkaClient networkClient, Optional<? extends Reconfigurable> reconfigurableChannelBuilder) {
    this.config = config;
    this.socketTimeout = socketTimeout;
    this.time = time;
    this.networkClient = networkClient;
    this.reconfigurableChannelBuilder = reconfigurableChannelBuilder;
    this.targetNode = targetBroker;
  }

  /**
   * @throws ConnectionException - if the connection to the node could not be established
   * @throws IOException - if any exception occurred when sending/receiving the request
   */
  @Override
  public ClientResponse sendRequest(AbstractRequest.Builder<? extends AbstractRequest> requestBuilder) throws IOException {
    boolean connected;
    try {
      connected = NetworkClientUtils.awaitReady(networkClient, targetNode, time, socketTimeout);
    } catch (Exception e) {
      networkClient.close(targetNode.idString());
      throw new ConnectionException(String.format("Failed to establish connection to node %d", targetNode.id()), e);
    }
    if (!connected) {
      networkClient.close(targetNode.idString());
      throw new ConnectionException(String.format("Failed to connect to node %d within %d ms", targetNode.id(), socketTimeout),
          new SocketTimeoutException()
      );
    }

    try {
      ClientRequest clientRequest = networkClient.newClientRequest(targetNode.idString(),
          requestBuilder, time.milliseconds(), true);
      return NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time);
    } catch (IOException e) {
      networkClient.close(targetNode.idString());
      throw e;
    }
  }

  /**
   * Sends a #{@link InitiateShutdownRequest} to the #{@code targetNode}.
   * See #{@link #sendRequest(AbstractRequest.Builder)} for details.
   */
  public InitiateShutdownResponse sendShutdownRequest(InitiateShutdownRequest.Builder shutdownRequestBuilder) throws IOException {
    return (InitiateShutdownResponse) sendRequest(shutdownRequestBuilder).responseBody();
  }

  @Override
  public void initiateClose() {
    reconfigurableChannelBuilder.ifPresent(config::removeReconfigurable);
    networkClient.initiateClose();
  }

  @Override
  public void close() throws IOException {
    networkClient.close();
  }
}
