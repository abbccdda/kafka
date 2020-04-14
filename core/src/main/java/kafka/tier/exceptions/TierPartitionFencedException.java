/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.exceptions;

public class TierPartitionFencedException extends RuntimeException {

    public TierPartitionFencedException(final String message) {
        super(message);
    }

    public TierPartitionFencedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
