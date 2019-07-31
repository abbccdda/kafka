/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import org.apache.kafka.common.record.AbstractLegacyRecordBatch;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.kafka.common.record.DefaultRecordBatch.PARTITION_LEADER_EPOCH_LENGTH;
import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.MAGIC_LENGTH;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

public class TierSegmentReader {

    /**
     * Loads records from a given InputStream up to maxBytes. This method will return at least one
     * record batch, regardless of the maxBytes setting. This method also will not return any
     * partial or incomplete record batches with respect to the record batch size defined in the
     * record batch header.
     * <p>
     * In the event that maxOffset is hit, this method can return an empty buffer.
     * <p>
     * Cancellation can be triggered using the CancellationContext, and it's granularity is on the
     * individual record batch level. That's to say, cancellation must wait for the current record
     * batch to be parsed and loaded (or ignored) before taking effect.
     */
    public static MemoryRecords loadRecords(CancellationContext cancellationContext,
                                            InputStream inputStream,
                                            int maxBytes,
                                            long targetOffset) throws IOException {

        RecordBatch firstBatch = null;
        // skip over batches < targetOffset
        while (!cancellationContext.isCancelled()) {
            try {
                RecordBatch recordBatch = readBatch(inputStream);
                if (recordBatch.baseOffset() <= targetOffset && recordBatch.lastOffset() >= targetOffset) {
                    firstBatch = recordBatch;
                    break;
                }
            } catch (EOFException e) {
                return MemoryRecords.EMPTY;
            }
        }

        if (firstBatch == null) {
            return MemoryRecords.EMPTY;
        }

        final int firstBatchSize = firstBatch.sizeInBytes();
        final int totalRequestBytes = Math.max(firstBatchSize, maxBytes);
        final ByteBuffer buffer = ByteBuffer.allocate(totalRequestBytes);
        firstBatch.writeTo(buffer);

        while (!cancellationContext.isCancelled() && buffer.position() < buffer.limit()) {
            final int positionCheckpoint = buffer.position();
            try {
                readBatchInto(inputStream, buffer);
            } catch (IOException | IndexOutOfBoundsException ignored) {
                buffer.position(positionCheckpoint);
                break;
            }
        }

        buffer.flip();
        return new MemoryRecords(buffer);
    }

    /**
     * Read the supplied input stream to find the first offset with a timestamp >= targetTimestamp
     *
     * @param cancellationContext cancellation context to allow reads of InputStream to be aborted
     * @param inputStream         InputStream for the tiered segment
     * @param targetTimestamp     target timestamp to lookup the offset for
     * @return Optional<Long> containing the offset for the supplied target timestamp
     * @throws IOException
     */
    public static Optional<Long> offsetForTimestamp(CancellationContext cancellationContext,
                                                    InputStream inputStream,
                                                    long targetTimestamp) throws IOException {
        while (!cancellationContext.isCancelled()) {
            final RecordBatch recordBatch = readBatch(inputStream);
            if (recordBatch.maxTimestamp() >= targetTimestamp) {
                for (final Record record : recordBatch) {
                    if (record.timestamp() >= targetTimestamp)
                        return Optional.of(record.offset());
                }
            }
        }
        return Optional.empty();
    }

    private static MagicAndBatchSizePair readMagicAndBatchSize(ByteBuffer buffer,
                                                               int headerStartPosition) {
        final byte magic = buffer.get(headerStartPosition + MAGIC_OFFSET);
        final int extraLength =
                HEADER_SIZE_UP_TO_MAGIC - PARTITION_LEADER_EPOCH_LENGTH - MAGIC_LENGTH;
        int batchSize = buffer.getInt(headerStartPosition + SIZE_OFFSET) + extraLength;
        return new MagicAndBatchSizePair(magic, batchSize);
    }

    /**
     * Reads one full batch from an InputStream. This method allocates twice per invocation,
     * once to read the header and again to allocate enough space to store the record batch
     * corresponding to the header.
     * <p>
     * Throws EOFException if either the header or full record batch cannot be read.
     */
    public static RecordBatch readBatch(InputStream inputStream) throws IOException {
        final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);
        final int bytesRead = Utils.readBytes(inputStream, logHeaderBuffer, HEADER_SIZE_UP_TO_MAGIC);
        if (bytesRead < HEADER_SIZE_UP_TO_MAGIC)
            throw new EOFException("Could not read HEADER_SIZE_UP_TO_MAGIC from InputStream");

        logHeaderBuffer.rewind();

        final MagicAndBatchSizePair magicAndBatchSizePair = readMagicAndBatchSize(logHeaderBuffer, 0);
        final byte magic = magicAndBatchSizePair.magic;
        final int batchSize = magicAndBatchSizePair.batchSize;

        final ByteBuffer recordBatchBuffer = ByteBuffer.allocate(batchSize);
        recordBatchBuffer.put(logHeaderBuffer);
        final int bytesToRead = recordBatchBuffer.limit() - recordBatchBuffer.position();
        final int recordBatchBytesRead = Utils.readBytes(inputStream, recordBatchBuffer, bytesToRead);

        if (recordBatchBytesRead < bytesToRead)
            throw new EOFException("Attempted to read a record batch of size " + batchSize +
                    " but was only able to read " + recordBatchBytesRead + " bytes");

        recordBatchBuffer.rewind();
        RecordBatch recordBatch;
        if (magic < RecordBatch.MAGIC_VALUE_V2)
            recordBatch = new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(recordBatchBuffer);
        else
            recordBatch = new DefaultRecordBatch(recordBatchBuffer);

        return recordBatch;
    }

    private static class MagicAndBatchSizePair {
        final byte magic;
        final int batchSize;

        private MagicAndBatchSizePair(byte magic, int batchSize) {
            this.magic = magic;
            this.batchSize = batchSize;
        }
    }

    /**
     * Similar to readBatch(), this method reads a full RecordBatch. The difference being, this
     * method reads into ByteBuffer, avoiding extra allocations. This method only advances the
     * position of the ByteBuffer if a full record batch is read.
     * <p>
     * Throws EOFException if either the header or full record batch cannot be read.
     */
    public static RecordBatch readBatchInto(InputStream inputStream, ByteBuffer buffer) throws IOException {
        final int startingPosition = buffer.position();
        final int bytesRead = Utils.readBytes(inputStream, buffer, HEADER_SIZE_UP_TO_MAGIC);
        if (bytesRead < HEADER_SIZE_UP_TO_MAGIC) {
            buffer.position(startingPosition);
            throw new EOFException("Could not read HEADER_SIZE_UP_TO_MAGIC from InputStream");
        }

        final MagicAndBatchSizePair magicAndBatchSizePair = readMagicAndBatchSize(buffer, startingPosition);
        final byte magic = magicAndBatchSizePair.magic;
        final int batchSize = magicAndBatchSizePair.batchSize;
        final int recordBatchBytesRead = Utils.readBytes(inputStream, buffer, batchSize - HEADER_SIZE_UP_TO_MAGIC);

        if (recordBatchBytesRead < batchSize - HEADER_SIZE_UP_TO_MAGIC) {
            buffer.position(startingPosition);
            throw new EOFException("Attempted to read a record batch of size " + batchSize +
                    " but was only able to read " + recordBatchBytesRead + " bytes");
        }

        final int currentPosition = buffer.position();
        buffer.position(startingPosition);
        ByteBuffer duplicate = buffer.slice();
        buffer.position(currentPosition);
        duplicate.limit(currentPosition - startingPosition);

        if (magic < RecordBatch.MAGIC_VALUE_V2)
            return new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(duplicate);
        else
            return new DefaultRecordBatch(duplicate);
    }
}
