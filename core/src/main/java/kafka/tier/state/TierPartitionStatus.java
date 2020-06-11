/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import java.util.HashMap;
import java.util.Map;

public enum TierPartitionStatus {
    // TierPartitionState has been created but has not been initialized
    UNINITIALIZED((byte) 0),
    // TierPartitionState has been initialized and is open for read/write, but is not being materialized
    INIT((byte) 1),
    // TierPartitionState has been initialized and is open for read/write, but is being materialized
    CATCHUP((byte) 2),
    // TierPartitionState has been initialized and is open for read/write. It is being continuously materialized by the primary consumer.
    ONLINE((byte) 3),
    // Disk is offline. Used for JBOD support.
    DISK_OFFLINE((byte) 4),
    // TierPartitionState encountered an error during materialization.
    ERROR((byte) 5);

    final byte value;

    private static final Map<Byte, TierPartitionStatus> VALUE_TO_STATUS = new HashMap<>();

    static {
        for (TierPartitionStatus status : TierPartitionStatus.values()) {
            TierPartitionStatus oldStatus = VALUE_TO_STATUS.put(status.value, status);
            if (oldStatus != null)
                throw new ExceptionInInitializerError(
                    "value reused for VALUE_TO_STATUS " + oldStatus + " and " + status);
        }
    }

    TierPartitionStatus(byte value) {
        this.value = value;
    }

    public boolean isOpen() {
        return this == INIT || this == CATCHUP || this == ONLINE || this == ERROR;
    }

    public boolean isOpenForWrite() {
        return this == INIT || this == CATCHUP || this == ONLINE;
    }

    public boolean hasError() {
        return this == ERROR;
    }

    public static byte toByte(TierPartitionStatus status) {
        return status.value;
    }

    public static TierPartitionStatus fromByte(byte value) {
        TierPartitionStatus status = VALUE_TO_STATUS.get(value);
        if (status == null)
            throw new IllegalArgumentException("Unknown TierPartitionStatus byte value " + value);
        return status;
    }
}
