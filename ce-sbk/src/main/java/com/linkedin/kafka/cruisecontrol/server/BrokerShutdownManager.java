/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.server;


import com.linkedin.kafka.cruisecontrol.client.BlockingSendClient;
import com.linkedin.kafka.cruisecontrol.client.ConnectionException;
import com.linkedin.kafka.cruisecontrol.common.AdminClientResult;
import com.linkedin.kafka.cruisecontrol.common.KafkaCluster;
import com.linkedin.kafka.cruisecontrol.common.SbkAdminUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import kafka.cluster.BrokerEndPoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitiateShutdownRequest;
import org.apache.kafka.common.requests.InitiateShutdownResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.BROKER_REMOVAL_SHUTDOWN_MS_CONFIG;

public class BrokerShutdownManager {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerShutdownManager.class);
  // the time to wait in between AdminClient#DescribeCluster calls when waiting to see the broker leave the cluster
  private static final int SHUTDOWN_WAIT_RETRY_DELAY_MS = 100;

  private final BlockingSendClient.Builder blockingSendClientBuilder;
  private final Time time;
  private final SbkAdminUtils adminUtils;
  private final long apiTimeoutMs;
  private final long shutdownWaitMs;

  public BrokerShutdownManager(SbkAdminUtils adminUtils, KafkaCruiseControlConfig config,
                               BlockingSendClient.Builder blockingSendClientBuilder,
                               Time time) {
    this.adminUtils = adminUtils;
    this.apiTimeoutMs = config.getInt(DEFAULT_API_TIMEOUT_MS_CONFIG);
    this.shutdownWaitMs = config.getLong(BROKER_REMOVAL_SHUTDOWN_MS_CONFIG);
    this.blockingSendClientBuilder = blockingSendClientBuilder;
    this.time = time;
  }

  /**
   * Ensures that the given broker is shut down by initiating shutdown via an #{@link InitiateShutdownRequest}
   * and awaiting the shutdown of the broker.
   *
   * This method is blocking for a long time. Approximately a couple of
   *  request timeouts (<code>request.timeout.ms</code>) and <code>broker.removal.shutdown.timeout.ms</code>.
   *
   * @throws TimeoutException - if a shutdown request was initiated but the broker does not shutdown within the timeout
   * @throws ApiException - if the shutdown request was not successful
   * @throws Exception - if the shutdown failed for any other reason
   * @return boolean indicating whether we shut down the broker by calling a request.
   *         if false, the broker was already shut down and no request was sent.
   */
  public boolean maybeShutdownBroker(int brokerId, Optional<Long> brokerEpochOpt) throws Exception {
    AdminClientResult<KafkaCluster> clusterResult = adminUtils.describeCluster(apiTimeoutMs);
    if (clusterResult.hasException()) {
      throw new ExecutionException("Failed to describe the cluster", clusterResult.exception());
    }

    KafkaCluster cluster = clusterResult.result();
    Optional<Node> brokerToShutDownOpt = cluster.nodes().stream().filter(node -> node.id() == brokerId).findAny();
    if (!brokerToShutDownOpt.isPresent()) {
      // broker is not part of the cluster already
      LOG.info("Skipping shutdown of broker {} because it's not part of the cluster.", brokerId);
      return false;
    } else if (!brokerEpochOpt.isPresent()) {
      String errMsg = String.format("Cannot shut down broker %d because no broker epoch was given.", brokerId);
      LOG.info(errMsg);
      throw new IllegalArgumentException(errMsg);
    }

    Node broker = brokerToShutDownOpt.get();
    BlockingSendClient shutdownClient = blockingSendClientBuilder.build(
        new BrokerEndPoint(broker.id(), broker.host(), broker.port())
    );
    shutdownBroker(shutdownClient, brokerId, brokerEpochOpt.get());
    return true;
  }

  /**
   * Initiates a shutdown on the broker #{@code brokerId}
   * and waits for the broker to shutdown and leave the cluster.
   *
   * This method is blocking.
   *
   * @throws TimeoutException - if the broker does not shutdown within the timeout
   * @throws ApiException - if the shutdown request was not successful
   * @throws ExecutionException - if any unexpected exception happens while sending/receiving the shutdown request
   */
  void shutdownBroker(BlockingSendClient shutdownClient, int brokerId, long brokerEpoch)
      throws ExecutionException, ApiException, TimeoutException, InterruptedException {
    String brokerStr = String.format("broker %d (epoch %d)", brokerId, brokerEpoch);

    InitiateShutdownResponse response = null;
    try {
      response = shutdownClient.sendShutdownRequest(new InitiateShutdownRequest.Builder(brokerEpoch));
    } catch (ConnectionException ce) {
      String errMsg = String.format("Failed to connect to %s while trying to send shutdown request.", brokerStr);
      LOG.error(errMsg, ce);
      throw new ExecutionException(errMsg, ce);
    } catch (IOException e) {
      // because it's a race condition of whether the broker can empty out its response queue before shutting down,
      // we handle disconnects and assume they're successful shutdown initiations
      LOG.info("Caught IOException (message: {}) while trying to shutdown {}." +
          "Assuming that the broker did not manage to respond before shutting down...", e.getMessage(), brokerStr);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Unexpected exception occurred while trying to send shutdown request for %s", brokerStr), e);
    }

    if (response != null && response.data().errorCode() != Errors.NONE.code()) {
      ApiException exception = Errors.forCode(response.data().errorCode()).exception();
      LOG.error("Failed shutting down broker due to exception in shutdown request", exception);
      throw exception;
    }

    awaitBrokerShutdown(shutdownWaitMs, brokerId);
  }

  /**
   * Waits for approximately #{@code shutdownWaitMs} for broker #{@code removeBrokerId} to shut down and leave the cluster.
   *
   * @throws TimeoutException - if the broker was still part of the cluster after #{@code shutdownWaitMs}
   */
  void awaitBrokerShutdown(long shutdownWaitMs, int removedBrokerId)
      throws InterruptedException, TimeoutException {
    long startTimeMs = time.milliseconds();

    while (true) {
      AdminClientResult<KafkaCluster> clusterResult = adminUtils.describeCluster(apiTimeoutMs);
      long nowMs = time.milliseconds();
      long elapsedTimeMs = nowMs - startTimeMs;

      if (clusterResult.hasException()) {
        LOG.warn("Failed to describe the cluster while awaiting broker {} shutdown. Retrying in {}ms",
            removedBrokerId, SHUTDOWN_WAIT_RETRY_DELAY_MS, clusterResult.exception());
      } else {
        KafkaCluster cluster = clusterResult.result();
        boolean brokerExists = cluster.nodes().stream().anyMatch(node -> node.id() == removedBrokerId);
        if (brokerExists) {
          LOG.debug("Broker {} is still part of the cluster ({}ms after the shutdown initiation)",
              removedBrokerId, elapsedTimeMs);
        } else {
          LOG.info("Broker {} has left the cluster successfully ({}ms after shutdown initiation)",
              removedBrokerId, elapsedTimeMs);
          return; // broker has shut down successfully!
        }
      }

      if (elapsedTimeMs >= shutdownWaitMs) {
        throw new TimeoutException(String.format("Timed out after waiting for broker %d to shutdown for %dms",
            removedBrokerId, shutdownWaitMs));
      }

      time.sleep(SHUTDOWN_WAIT_RETRY_DELAY_MS);
    }
  }
}
