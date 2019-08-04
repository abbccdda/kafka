// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.server.interceptor;

import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

public interface TopicMetadataListener {
    void topicMetadataUpdated(Collection<TopicPartition> partitions);
}
