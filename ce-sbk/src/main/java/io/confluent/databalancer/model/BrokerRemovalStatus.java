/**
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.model;

import java.util.Objects;

/**
 * Class to store status of broker removal progress. This can be then queried by
 * user to monitor the progress of the broker removal operation.
 */
public class BrokerRemovalStatus {

    private final int brokerId; // Id of the broker getting removed
    private final Exception ex;

    public BrokerRemovalStatus(int brokerId, Exception ex) {
        this.brokerId = brokerId;
        this.ex = ex;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public Exception getException() {
        return ex;
    }

    @Override
    public String toString() {
        return "BrokerRemovalStatus{" +
                "brokerId=" + brokerId +
                ", ex=" + ex +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrokerRemovalStatus that = (BrokerRemovalStatus) o;

        if (brokerId != that.brokerId) return false;
        return Objects.equals(ex, that.ex);
    }

    @Override
    public int hashCode() {
        int result = brokerId;
        result = 31 * result + (ex != null ? ex.hashCode() : 0);
        return result;
    }
}
