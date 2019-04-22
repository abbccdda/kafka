/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.serdes.ObjectMetadata;
import org.apache.kafka.common.TopicPartition;
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
    private final TopicPartition topicPartition;

    private long position;
    private long endPosition;
    private FileChannel channel;
    private ByteBuffer entryBuffer = null;

    public FileTierPartitionIterator(TopicPartition topicPartition,
                                     FileChannel channel,
                                     long startPosition) throws IOException {
        this.topicPartition = topicPartition;
        this.channel = channel;
        this.position = startPosition;
        this.endPosition = channel.size();
    }

    @Override
    protected TierObjectMetadata makeNext() {
        if (position >= endPosition)
            return allDone();

        try {
            long currentPosition = position;

            if (!fillLengthBuffer(currentPosition))
                throw new IOException("Failed to read TierObjectMetadata length prefix.");

            final short length = lengthBuffer.getShort();
            currentPosition += lengthBuffer.limit();

            // reallocate entry buffer if needed
            if (entryBuffer == null || length > entryBuffer.capacity()) {
                if (entryBuffer != null)
                    log.debug("Resizing tier partition state iterator buffer from " + entryBuffer.capacity() + " to " + length);

                entryBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
            }

            if (!fillEntryBuffer(currentPosition, length))
                throw new IOException("Failed to read TierObjectMetadata of length " + length);

            currentPosition += entryBuffer.limit();

            // advance position
            position = currentPosition;

            return new TierObjectMetadata(topicPartition, ObjectMetadata.getRootAsObjectMetadata(entryBuffer));
        } catch (IOException e) {
            throw new KafkaStorageException(e);
        }
    }

    /**
     * Fills object metadata entry buffer.
     * @param currentPosition file channel position of object metadata entry
     * @param length length of object metadata entry
     * @return boolean for whether the buffer was successfully filled with expected entry size
     * @throws IOException
     */
    private boolean fillEntryBuffer(long currentPosition, short length) throws IOException {
        entryBuffer.clear();
        Utils.readFully(channel, entryBuffer, currentPosition);
        entryBuffer.flip();

        return entryBuffer.limit() == length;
    }

    /**
     * Fills object metadata length buffer.
     * @param currentPosition file channel position of object metadata length prefix
     * @return boolean for whether the length buffer was successfully filled with length prefix size
     * @throws IOException
     */
    private boolean fillLengthBuffer(long currentPosition) throws IOException {
        lengthBuffer.clear();
        Utils.readFully(channel, lengthBuffer, currentPosition);
        lengthBuffer.flip();

        return lengthBuffer.limit() == ENTRY_LENGTH_SIZE;
    }

    public long position() {
        return position;
    }
}
