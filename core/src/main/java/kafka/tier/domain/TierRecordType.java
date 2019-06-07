/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import java.util.HashMap;
import java.util.Map;

public enum TierRecordType {
    InitLeader((byte) 0),
    SegmentUploadInitiate((byte) 1),
    SegmentUploadComplete((byte) 2),
    SegmentDeleteInitiate((byte) 3),
    SegmentDeleteComplete((byte) 4);

    private static final Map<Byte, TierRecordType> ID_TO_TYPE = new HashMap<>();
    private final byte id;

    static {
        for (TierRecordType type : TierRecordType.values()) {
            TierRecordType oldType = ID_TO_TYPE.put(type.id, type);
            if (oldType != null)
                throw new ExceptionInInitializerError("id reused for ID_TO_TYPE " + oldType + " and " + type);
        }
    }

    TierRecordType(byte id) {
        this.id = id;
    }

    public static byte toByte(TierRecordType recordType) {
        return recordType.id;
    }

    public static TierRecordType toType(byte id) {
        TierRecordType type = ID_TO_TYPE.get(id);
        if (type == null)
            throw new IllegalArgumentException("Unknown id " + id);
        return type;
    }
}