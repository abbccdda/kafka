package org.apache.kafka.common.errors;

/**
 * Thrown if a broker removal request for the specific broker is currently being executed
 */
public class BrokerRemovalInProgressException extends ApiException {
    public BrokerRemovalInProgressException(String message) {
        super(message);
    }
}
