/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

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
}
