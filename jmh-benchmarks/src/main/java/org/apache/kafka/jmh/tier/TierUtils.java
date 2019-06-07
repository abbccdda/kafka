/*
 Copyright 2018 Confluent Inc.
 */

package org.apache.kafka.jmh.tier;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierSegmentUploadComplete;
import kafka.tier.domain.TierSegmentUploadInitiate;
import kafka.tier.state.TierPartitionState;

import java.io.IOException;
import java.util.UUID;

public class TierUtils {
    public static TierPartitionState.AppendResult uploadWithMetadata(TierPartitionState tierPartitionState,
                                                                     TopicIdPartition topicIdPartition,
                                                                     int tierEpoch,
                                                                     UUID objectId,
                                                                     long startOffset,
                                                                     long endOffset,
                                                                     long maxTimestamp,
                                                                     int size,
                                                                     boolean hasAbortedTxns,
                                                                     boolean hasEpochState,
                                                                     boolean hasProducerState) throws IOException {
        TierSegmentUploadInitiate uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition,
                tierEpoch,
                objectId,
                startOffset,
                endOffset,
                maxTimestamp,
                size,
                hasEpochState,
                hasAbortedTxns,
                hasProducerState);

        TierPartitionState.AppendResult result = tierPartitionState.append(uploadInitiate);
        if (result != TierPartitionState.AppendResult.ACCEPTED) {
            return result;
        } else {
            TierSegmentUploadComplete uploadComplete = new TierSegmentUploadComplete(uploadInitiate);
            return tierPartitionState.append(uploadComplete);
        }
    }
}
