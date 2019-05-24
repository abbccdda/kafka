package kafka.tier;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;
import java.util.UUID;

final public class TopicIdPartition {
    private final UUID topicId;
    private final TopicPartition topicPartition;

    public TopicIdPartition(String topic, UUID topicId, int partition) {
        topicPartition = new TopicPartition(topic, partition);
        this.topicId = topicId;
    }

    public int partition() {
        return topicPartition.partition();
    }

    public String topic() {
        return topicPartition.topic();
    }

    public UUID topicId() {
        return topicId;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, topicId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicIdPartition other = (TopicIdPartition) obj;
        return Objects.equals(this.topicPartition(), other.topicPartition())
                && Objects.equals(this.topicId(), other.topicId());
    }

    @Override
    public String toString() {
        return topic() + "-" + topicId() + "-" + partition();
    }

}
