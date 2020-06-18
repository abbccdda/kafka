// (Copyright) [2020 - 2020] Confluent, Inc.
package org.apache.kafka.common.requests;

import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;

// Abstract class for all control responses including UpdateMetadataRequest, LeaderAndIsrRequest and StopReplicaRequest
public abstract class AbstractControlResponse extends AbstractResponse {

    public abstract Errors error();

    public abstract Map<TopicPartition, Errors> partitionErrors();
}
