/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.serdes.ObjectMetadata;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class FileTierPartitionIterator extends AbstractIterator<TierObjectMetadata> {
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionIterator.class);
    private static final int ENTRY_LENGTH_SIZE = 2;

    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(ENTRY_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    private final TopicIdPartition topicIdPartition;

    private long position;
    private long endPosition;
    private FileChannel channel;
    private ByteBuffer entryBuffer = null;

    public FileTierPartitionIterator(TopicIdPartition topicIdPartition,
                                     FileChannel channel,
                                     long startPosition) throws IOException {
        this.topicIdPartition = topicIdPartition;
        this.channel = channel;
        this.position = startPosition;
        this.endPosition = channel.size();
    }

    @Override
    protected TierObjectMetadata makeNext() {
        if (position >= endPosition)
            return allDone();

        try {
            long currentPosition = this.position;
            // read length
            Utils.readFully(channel, lengthBuffer, currentPosition);
            if (lengthBuffer.hasRemaining())
                return allDone();
            currentPosition += lengthBuffer.limit();
            lengthBuffer.flip();

            short length = lengthBuffer.getShort();

            // check if we have enough bytes to read the entry
            if (currentPosition + length > endPosition)
                return allDone();

            // reallocate entry buffer if needed
            if (entryBuffer == null || length > entryBuffer.capacity()) {
                if (entryBuffer != null)
                    log.debug("Resizing tier partition state iterator buffer from " + entryBuffer.capacity() + " to " + length);

                entryBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
            }

            // read and return the entry
            entryBuffer.clear();
            entryBuffer.limit(length);
            Utils.readFully(channel, entryBuffer, currentPosition);
            if (entryBuffer.hasRemaining())
                return allDone();
            currentPosition += entryBuffer.limit();
            entryBuffer.flip();

            // advance position
            position = currentPosition;

            return new TierObjectMetadata(topicIdPartition, ObjectMetadata.getRootAsObjectMetadata(entryBuffer));
        } catch (IOException e) {
            throw new KafkaStorageException(e);
        }
    }

    public long position() {
        return position;
    }
}
