package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import java.util.Map;
import java.util.UUID;

public interface LeaderAndIsrRequestBase {
    Map<String, UUID> topicIds();

    Map<TopicPartition, LeaderAndIsrRequest.PartitionState> partitionStates();

    AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e);

    int controllerId();

    int controllerEpoch();
}
