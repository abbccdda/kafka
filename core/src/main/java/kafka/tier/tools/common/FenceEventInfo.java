package kafka.tier.tools.common;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Encapsulates information related to a fencing event. Provides a `toJson()` API that returns a
 * JSON-formatted string representation of an object of this class.
 */
public class FenceEventInfo {
    public final String topic;

    public final String topicIdBase64;

    public final int partition;

    public final String recordMessageIdBase64;

    public final long recordOffset;

    private static final ObjectMapper JSON_SERDE;
    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    }

    @JsonCreator
    public FenceEventInfo(
        @JsonProperty("topic") String topic,
        @JsonProperty("topicIdBase64") String topicIdBase64,
        @JsonProperty("partition") int partition,
        @JsonProperty("recordMessageIdBase64") String recordMessageIdBase64,
        @JsonProperty("recordOffset") long recordOffset
    ) {
        this.topic = topic;
        this.topicIdBase64 = topicIdBase64;
        this.partition = partition;
        this.recordMessageIdBase64 = recordMessageIdBase64;
        this.recordOffset = recordOffset;
    }

    @JsonProperty(value = "topic", required = true)
    public String topic() {
        return topic;
    }

    @JsonProperty(value = "topicIdBase64", required = true)
    public String topicIdBase64() {
        return topicIdBase64;
    }

    @JsonProperty(value = "partition", required = true)
    public int partition() {
        return partition;
    }

    @JsonProperty(value = "recordMessageIdBase64", required = true)
    public String recordMessageIdBase64() {
        return recordMessageIdBase64;
    }

    @JsonProperty(value = "recordOffset", required = true)
    public long recordOffset() {
        return recordOffset;
    }

    public String toJson() {
        try {
            return JSON_SERDE.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String listToJson(List<FenceEventInfo> events) {
        try {
            return JSON_SERDE.writeValueAsString(events);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FenceEventInfo)) {
            return false;
        }
        FenceEventInfo fenceEventInfo = (FenceEventInfo) o;
        return Objects.equals(topic, fenceEventInfo.topic) &&
            Objects.equals(topicIdBase64, fenceEventInfo.topicIdBase64) &&
            Objects.equals(partition, fenceEventInfo.partition) &&
            Objects.equals(recordMessageIdBase64, fenceEventInfo.recordMessageIdBase64) &&
            Objects.equals(recordOffset, fenceEventInfo.recordOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, topicIdBase64, partition, recordMessageIdBase64, recordOffset);
    }

    public String toString() {
        return String.format(
            "FenceEventInfo(topic=%s, topicIdBase64=%s, partition=%d," +
            " recordMessageIdBase64=%s, recordOffset=%d)",
            topic(),
            topicIdBase64(),
            partition(),
            recordMessageIdBase64(),
            recordOffset()
        );
    }
}

