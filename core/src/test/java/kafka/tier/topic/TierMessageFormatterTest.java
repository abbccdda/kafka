package kafka.tier.topic;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierPartitionDeleteInitiate;
import kafka.tier.domain.TierRecordType;
import kafka.tier.domain.TierSegmentDeleteComplete;
import kafka.tier.domain.TierSegmentDeleteInitiate;
import kafka.tier.domain.TierSegmentUploadComplete;
import kafka.tier.domain.TierSegmentUploadInitiate;
import kafka.tier.domain.TierTopicInitLeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;


public class TierMessageFormatterTest {

    private TierMessageFormatter formatter = new TierMessageFormatter();

    @Test
    public void formatTierTopicInitLeaderTest() {
        UUID topicId = UUID.randomUUID();
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", topicId, 0);
        UUID messageId = UUID.randomUUID();
        AbstractTierMetadata initLeader = new TierTopicInitLeader(topicIdPartition, 1, messageId, 0);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "foo",
                topicIdPartition.partition(),
                0,
                System.currentTimeMillis(),
                TimestampType.LOG_APPEND_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                initLeader.serializeKey(),
                initLeader.serializeValue());
        formatter.writeTo(record, ps);

        String expected = String.format("(%d, %d, %s): %s\n",
                topicIdPartition.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()), initLeader.toString());
        assertEquals(expected, baos.toString());
    }

    @Test
    public void formatTierPartitionDeleteInitiateTest() {
        UUID topicId = UUID.randomUUID();
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", topicId, 0);
        UUID messageId = UUID.randomUUID();
        AbstractTierMetadata partDeleteInit = new TierPartitionDeleteInitiate(topicIdPartition, 1, messageId);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "foo",
                topicIdPartition.partition(),
                1,
                System.currentTimeMillis(),
                TimestampType.LOG_APPEND_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                partDeleteInit.serializeKey(),
                partDeleteInit.serializeValue());
        formatter.writeTo(record, ps);

        String expected = String.format("(%d, %d, %s): %s\n",
                topicIdPartition.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()), partDeleteInit.toString());
        assertEquals(expected, baos.toString());
    }

    @Test
    public void formatTierSegmentDeleteTest() {
        UUID topicId = UUID.randomUUID();
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", topicId, 0);
        AbstractTierMetadata segDeleteInit = new TierSegmentDeleteInitiate(topicIdPartition, 1, UUID.randomUUID());
        AbstractTierMetadata segDeleteComplete = new TierSegmentDeleteComplete(topicIdPartition, 1, UUID.randomUUID());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ConsumerRecord<byte[], byte[]> initRecord = new ConsumerRecord<>(
                "foo",
                topicIdPartition.partition(),
                1,
                System.currentTimeMillis(),
                TimestampType.LOG_APPEND_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                segDeleteInit.serializeKey(),
                segDeleteInit.serializeValue());
        formatter.writeTo(initRecord, ps);
        ConsumerRecord<byte[], byte[]> completeRecord = new ConsumerRecord<>(
                "foo",
                topicIdPartition.partition(),
                2,
                System.currentTimeMillis(),
                TimestampType.LOG_APPEND_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                segDeleteComplete.serializeKey(),
                segDeleteComplete.serializeValue());
        formatter.writeTo(completeRecord, ps);

        String expected = String.format("(%d, %d, %s): %s\n(%d, %d, %s): %s\n",
                topicIdPartition.partition(), initRecord.offset(), Instant.ofEpochMilli(initRecord.timestamp()), segDeleteInit.toString(),
                topicIdPartition.partition(), completeRecord.offset(), Instant.ofEpochMilli(completeRecord.timestamp()), segDeleteComplete.toString());
        assertEquals(expected, baos.toString());
    }

    @Test
    public void formatTierSegmentUploadTest() {
        UUID topicId = UUID.randomUUID();
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", topicId, 0);
        AbstractTierMetadata segUploadInit = new TierSegmentUploadInitiate(topicIdPartition, 1, UUID.randomUUID(), 0, 0, 0, 0, false, false, false);
        AbstractTierMetadata segUploadComplete = new TierSegmentUploadComplete(topicIdPartition, 1, UUID.randomUUID());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ConsumerRecord<byte[], byte[]> initRecord = new ConsumerRecord<>(
                "foo",
                topicIdPartition.partition(),
                1,
                System.currentTimeMillis(),
                TimestampType.LOG_APPEND_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                segUploadInit.serializeKey(),
                segUploadInit.serializeValue());
        formatter.writeTo(initRecord, ps);
        ConsumerRecord<byte[], byte[]> completeRecord = new ConsumerRecord<>(
                "foo",
                topicIdPartition.partition(),
                2,
                System.currentTimeMillis(),
                TimestampType.LOG_APPEND_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                segUploadComplete.serializeKey(),
                segUploadComplete.serializeValue());
        formatter.writeTo(completeRecord, ps);

        String expected = String.format("(%d, %d, %s): %s\n(%d, %d, %s): %s\n",
                topicIdPartition.partition(), initRecord.offset(), Instant.ofEpochMilli(initRecord.timestamp()), segUploadInit.toString(),
                topicIdPartition.partition(), completeRecord.offset(), Instant.ofEpochMilli(completeRecord.timestamp()), segUploadComplete.toString());
        assertEquals(expected, baos.toString());
    }

    private class UnknownTierMetadata extends AbstractTierMetadata {

        @Override
        public TierRecordType type() {
            return null;
        }

        @Override
        public TopicIdPartition topicIdPartition() {
            return new TopicIdPartition("foo", UUID.randomUUID(), 0);
        }

        @Override
        public ByteBuffer payloadBuffer() {
            return null;
        }

        @Override
        public int tierEpoch() {
            return 0;
        }

        @Override
        public UUID messageId() {
            return UUID.randomUUID();
        }

        @Override
        public byte[] serializeValue() {
            return new byte[]{-1};
        }
    }

    @Test
    public void formatDeserializationFailureTest() {
        AbstractTierMetadata unknown = new UnknownTierMetadata();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "foo",
                0,
                1,
                System.currentTimeMillis(),
                TimestampType.LOG_APPEND_TIME,
                ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                unknown.serializeKey(),
                unknown.serializeValue());
        formatter.writeTo(record, ps);

        String expected = String.format("(%d, %d, %s): failed to deserialize tier metadata. Error message: Unknown id -1. Record: %s\n",
                record.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()), record.toString());
        assertEquals(expected, baos.toString());
    }
}
