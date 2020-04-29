package org.apache.kafka.common.errors;

/**
 * Thrown if a broker removal request for the specific broker has been previously completed
 */
public class BrokerRemovedException extends ApiException {
    public BrokerRemovedException(String message) {
        super(message);
    }
}
