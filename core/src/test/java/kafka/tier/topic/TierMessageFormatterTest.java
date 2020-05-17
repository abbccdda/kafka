package kafka.tier.topic;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierPartitionFence;
import kafka.tier.domain.TierPartitionDeleteInitiate;
import kafka.tier.domain.TierPartitionForceRestore;
import kafka.tier.domain.TierRecordType;
import kafka.tier.domain.TierSegmentDeleteComplete;
import kafka.tier.domain.TierSegmentDeleteInitiate;
import kafka.tier.domain.TierSegmentUploadComplete;
import kafka.tier.domain.TierSegmentUploadInitiate;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.state.OffsetAndEpoch;
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
    public void formatTierPartitionForceRestoreTest() {
        UUID topicId = UUID.fromString("4da3c386-128c-48f3-bd2a-8c0e4ddc81c4");
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", topicId, 0);
        UUID messageId = UUID.fromString("71ad0b74-d8a3-487a-baf6-bb152d8f70d3");
        AbstractTierMetadata partitionRestore = new TierPartitionForceRestore(topicIdPartition,
                messageId, 1, 100, new OffsetAndEpoch(300L, Optional.of(30)), "contenthash");

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
                partitionRestore.serializeKey(),
                partitionRestore.serializeValue());
        formatter.writeTo(record, ps);

        String expected = String.format("(%d, %d, %s): %s\n",
                topicIdPartition.partition(), record.offset(),
                Instant.ofEpochMilli(record.timestamp()), partitionRestore.toString());
        assertEquals(expected, baos.toString());
        assertEquals("TierPartitionForceRestore(version=0, "
                        + "topicIdPartition=TaPDhhKMSPO9KowOTdyBxA-foo-0, "
                        + "messageIdAsBase64=ca0LdNijSHq69rsVLY9w0w, "
                        + "startOffset=1, endOffset=100, "
                        + "stateOffsetAndEpoch=OffsetAndEpoch(offset=300, "
                        + "epoch=Optional[30]), "
                        + "contentHash=contenthash)",
                partitionRestore.toString());
    }

    @Test
    public void formatTierSegmentDeleteTest() {
        UUID topicId = UUID.randomUUID();
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", topicId, 0);
        AbstractTierMetadata segDeleteInit = new TierSegmentDeleteInitiate(topicIdPartition, 1,
                UUID.randomUUID(), new OffsetAndEpoch(300L, Optional.of(30)));
        AbstractTierMetadata segDeleteComplete = new TierSegmentDeleteComplete(topicIdPartition,
                1, UUID.randomUUID(), new OffsetAndEpoch(300L, Optional.of(30)));

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
        AbstractTierMetadata segUploadInit = new TierSegmentUploadInitiate(topicIdPartition, 1,
                UUID.randomUUID(), 0, 0, 0, 0, false, false, false, new OffsetAndEpoch(300,
                Optional.empty()));
        AbstractTierMetadata segUploadComplete = new TierSegmentUploadComplete(topicIdPartition,
                1, UUID.randomUUID(), new OffsetAndEpoch(400, Optional.of(3)));

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

    @Test
    public void formatTierPartitionFenceTest() {
        UUID topicId = UUID.randomUUID();
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", topicId, 0);
        UUID messageId = UUID.randomUUID();
        AbstractTierMetadata partitionFence = new TierPartitionFence(topicIdPartition, messageId);

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
            partitionFence.serializeKey(),
            partitionFence.serializeValue());
        formatter.writeTo(record, ps);

        String expected = String.format("(%d, %d, %s): %s\n",
            topicIdPartition.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()), partitionFence.toString());
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
        public OffsetAndEpoch stateOffsetAndEpoch() {
            return OffsetAndEpoch.EMPTY;
        }

        @Override
        public UUID messageId() {
            return UUID.randomUUID();
        }

        @Override
        public byte[] serializeValue() {
            return new byte[]{-1};
        }

        @Override
        public int expectedSizeLatestVersion() {
            return 0;
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

        String expected = String.format("(%d, %d, %s): failed to deserialize tier metadata. Error message: Deserialization error [Unknown id -1]. Record: %s\n",
                record.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()), record.toString());
        assertEquals(expected, baos.toString());
    }
}
