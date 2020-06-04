/*
 * Copyright (C) 2020 Confluent Inc.
 */
package kafka.common;

/**
 * Class to store status of broker add progress. This can be then queried by
 * user to monitor the progress of the broker addition.
 */
public class BrokerAddStatus {

    private final int brokerId; // id of the broker getting added
    private final Exception exception;
    private long startTime;
    // Time at which status was last updated
    private long lastUpdateTime;

    public BrokerAddStatus(int brokerId, Exception exception) {
        this.brokerId = brokerId;
        this.exception = exception;
    }

    public int brokerId() {
        return brokerId;
    }

    public Exception getException() {
        return exception;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrokerAddStatus that = (BrokerAddStatus) o;

        return brokerId == that.brokerId;
    }

    @Override
    public int hashCode() {
        return brokerId;
    }
}
