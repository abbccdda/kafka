/*
 Copyright 2020 Confluent Inc.
 */

package io.confluent.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;

public final class RollHandler extends AbstractHandler {
    private static final Logger log = LoggerFactory.getLogger(InternalRestServer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final BeginShutdownBrokerHandle brokerHandle;

    public RollHandler(BeginShutdownBrokerHandle brokerHandle) {
        this.brokerHandle = brokerHandle;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
                       HttpServletResponse response) throws IOException {
        if (target.equals("/status")) {
            log.debug("Handling roll status query");
            handleStatusQuery(response);
        } else if (target.equals("/shutdown")) {
            log.debug("Handling roll shutdown request");
            handleShutdown(request, response);
        }
    }

    private void handleStatusQuery(HttpServletResponse response) throws IOException {
        try {
            final StatusResponse statusResponse = new StatusResponse(brokerHandle.brokerId(),
                    brokerHandle.underReplicatedPartitions(),
                    brokerHandle.controllerId(),
                    brokerHandle.brokerEpoch());
            ResponseContainer.dataResponse(statusResponse).write(OBJECT_MAPPER, response);
        } catch (Exception e) {
            log.error("Failed to retrieve and write broker status", e);
            ResponseContainer.ErrorResponse resp =
                    genericErrorResponse("Failed to retrieve and "
                    + "write broker status");
            ResponseContainer
                    .errorResponse(Collections.singletonList(resp))
                    .write(OBJECT_MAPPER, response);
        }
    }

    // For now, there is only 1 type of error response
    private static ResponseContainer.ErrorResponse genericErrorResponse(String message) {
        return new ResponseContainer.ErrorResponse(0,
                HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message);
    }

    private void handleShutdown(HttpServletRequest request,
                                HttpServletResponse response) throws IOException {
        try (final ServletInputStream inputStream = request.getInputStream()) {
            ShutdownRequest shutdownRequest = OBJECT_MAPPER.readValue(inputStream, ShutdownRequest.class);
            final long currentBrokerId = brokerHandle.brokerId();
            if (shutdownRequest.brokerId != currentBrokerId) {
                log.warn(String.format("shutdown request broker_id %d does not match current %d",
                        shutdownRequest.brokerId, currentBrokerId));
                final ResponseContainer.ErrorResponse errResponse = genericErrorResponse(
                        "broker_id does not match recipient broker");
                final ResponseContainer<?> responseContainer =
                        ResponseContainer.errorResponse(Collections.singletonList(errResponse));
                responseContainer.write(OBJECT_MAPPER, response);
            } else {
                try {
                    brokerHandle.beginShutdown(shutdownRequest.brokerEpoch);
                } catch (StaleBrokerEpochException ignored) {
                    log.warn(String.format("shutdown request broker_epoch %d does not match "
                            + "current brokerEpoch", shutdownRequest.brokerEpoch));
                    final ResponseContainer.ErrorResponse errResponse = genericErrorResponse(
                            "broker_epoch does not match recipient broker");
                    final ResponseContainer<?> responseContainer =
                            ResponseContainer.errorResponse(Collections.singletonList(errResponse));
                    responseContainer.write(OBJECT_MAPPER, response);
                    return;
                }
                log.info(String.format("beginShutdown successful for broker %d epoch %d",
                        shutdownRequest.brokerEpoch, shutdownRequest.brokerEpoch));
                // Shutdown was successful, return 200 with no body.
                response.setCharacterEncoding("UTF-8");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().close();
            }
        }
    }

    /**
     * Response to a roll status query.
     */
    final static class StatusResponse {
        /**
         * The brokerId of the queried broker.
         */
        @JsonProperty("broker_id")
        final long brokerId;

        /**
         * The current broker epoch of the queried broker.
         */
        @JsonProperty("broker_epoch")
        final long brokerEpoch;

        /**
         * The number of under replicated partitions present on the queried broker.
         */
        @JsonProperty("under_replicated_partitions")
        final long underReplicatedPartitions;

        /**
         * The brokerId of the current controller.
         */
        @JsonProperty("controller_id")
        final Integer controllerId;


        @JsonCreator
        public StatusResponse(@JsonProperty(value = "broker_id", required = true) long brokerId,
                              @JsonProperty(value = "under_replicated_partitions", required = true)
                                      long underReplicatedPartitions,
                              @JsonProperty(value = "controller_id", required = false)
                                              Integer controllerId,
                              @JsonProperty(value = "broker_epoch", required = true)
                                              long brokerEpoch) {
            this.brokerId = brokerId;
            this.brokerEpoch = brokerEpoch;
            this.underReplicatedPartitions = underReplicatedPartitions;
            this.controllerId = controllerId;
        }

        @Override
        public String toString() {
            return "StatusResponse{" +
                    "brokerId=" + brokerId +
                    ", brokerEpoch=" + brokerEpoch +
                    ", underReplicatedPartitions=" + underReplicatedPartitions +
                    ", controllerId=" + controllerId +
                    '}';
        }
    }

    /**
     * Request that a broker begins shutting down.
     */
    final static class ShutdownRequest {
        /**
         * The broker id for the broker which should shut down.
         */
        @JsonProperty("broker_id")
        final long brokerId;

        /**
         * The broker epoch for the broker which should shut down.
         */
        @JsonProperty("broker_epoch")
        final long brokerEpoch;

        @JsonCreator
        public ShutdownRequest(@JsonProperty(value = "broker_id", required = true)
                                               long brokerId,
                               @JsonProperty(value = "broker_epoch", required = true)
                                       long brokerEpoch) {
            this.brokerId = brokerId;
            this.brokerEpoch = brokerEpoch;
        }

        @Override
        public String toString() {
            return "ShutdownRequest{" +
                    "brokerId=" + brokerId +
                    ", brokerEpoch=" + brokerEpoch +
                    '}';
        }
    }
}
