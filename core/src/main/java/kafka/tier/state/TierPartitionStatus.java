/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

public enum TierPartitionStatus {
    // TierPartitionState has been initialized but it is not yet backed by a file on disk
    CLOSED((byte) 0),
    // TierPartitionState has been initialized and is open for read/write, but is not being materialized
    INIT((byte) 1),
    // TierPartitionState has been initialized and is open for read/write, but is being materialized
    CATCHUP((byte) 2),
    // TierPartitionState has been initialized and is open for read/write. It is being continuously materialized by the primary consumer.
    ONLINE((byte) 3),
    // Disk is offline. Used for JBOD support.
    DISK_OFFLINE((byte) 4);

    final byte value;

    TierPartitionStatus(byte value) {
        this.value = value;
    }

    public boolean isOpen() {
        return this == INIT || this == CATCHUP || this == ONLINE;
    }

    public boolean isOpenForWrite() {
        return this == INIT || this == CATCHUP || this == ONLINE;
    }

    public static byte toByte(TierPartitionStatus status) {
        return status.value;
    }

    public static TierPartitionStatus fromByte(byte value) {
        if (value == CLOSED.value)
            return CLOSED;
        else if (value == INIT.value)
            return INIT;
        else if (value == CATCHUP.value)
            return CATCHUP;
        else if (value == ONLINE.value)
            return ONLINE;
        else if (value == DISK_OFFLINE.value)
            return DISK_OFFLINE;
        else
            throw new IllegalArgumentException("Unrecognized TierPartitionStatus byte value " + value);
    }
}
