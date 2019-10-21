/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import kafka.utils.CoreUtils;

import java.util.UUID;

/**
 * Represents metadata for a particular segment.
 */
public abstract class AbstractTierSegmentMetadata extends AbstractTierMetadata {
    /**
     * The current state of the segment.
     */
    public abstract TierObjectMetadata.State state();

    /**
     * A unique identifier for the segment.
     */
    public final UUID objectId() {
        return messageId();
    }

    /**
     * Encode objectId with Base64
     * @return string representing objectId encoded in Base64
     */
    public final String objectIdAsBase64() {
        return CoreUtils.uuidToBase64(objectId());
    }
}
