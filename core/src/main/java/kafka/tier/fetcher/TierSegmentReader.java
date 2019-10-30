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
    private final CancellationContext cancellationContext;
    private final InputStream inputStream;
    private int finalBatchStartPosition = 0;
    private Long nextOffset = null;
    private Integer nextBatchSize = null;

    TierSegmentReader(CancellationContext cancellationContext, InputStream inputStream) {
        this.cancellationContext = cancellationContext;
        this.inputStream = inputStream;
    }

    public Long nextOffset() {
        return nextOffset;
    }

    public Integer nextBatchSize() {
        return nextBatchSize;
    }

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
    public MemoryRecords readRecords(int maxBytes, long targetOffset) throws IOException {
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

        if (firstBatch == null)
            return MemoryRecords.EMPTY;

        // insert the first batch, ensuring the buffer size to at least as big as the first
        // batch, even if this exceeds maxBytes
        final int firstBatchSize = firstBatch.sizeInBytes();
        final int totalRequestBytes = Math.max(firstBatchSize, maxBytes);
        final ByteBuffer buffer = ByteBuffer.allocate(totalRequestBytes);
        firstBatch.writeTo(buffer);

        // We have already read more than maxBytes in the first record batch.
        // Read the following record batch header to allow the next batch size to be cached
        if (firstBatchSize >= maxBytes) {
            // Read the next header to optimize the next range fetch
            final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);
            final int bytesRead = Utils.readBytes(inputStream, logHeaderBuffer, HEADER_SIZE_UP_TO_MAGIC);
            if (bytesRead == HEADER_SIZE_UP_TO_MAGIC)
                nextBatchSize = readMagicAndBatchSize(logHeaderBuffer, 0).batchSize;
        } else {
            readBatchesUpToMaxBytes(cancellationContext, inputStream, buffer);
        }

        buffer.flip();
        nextOffset = nextOffset(finalBatchStartPosition, buffer);
        return new MemoryRecords(buffer);
    }

    // Read additional batches from input stream up to maxBytes
    private void readBatchesUpToMaxBytes(CancellationContext cancellationContext, InputStream inputStream, ByteBuffer buffer) {
        while (!cancellationContext.isCancelled() && buffer.position() < buffer.limit()) {
            final int positionCheckpoint = buffer.position();
            try {
                final int bytesRead = Utils.readBytes(inputStream, buffer, HEADER_SIZE_UP_TO_MAGIC);
                if (bytesRead < HEADER_SIZE_UP_TO_MAGIC) {
                    buffer.position(positionCheckpoint);
                    throw new EOFException("Could not read HEADER_SIZE_UP_TO_MAGIC from InputStream");
                }

                final int batchSize = readMagicAndBatchSize(buffer, positionCheckpoint).batchSize;
                // store the current batch size in case we can't read the full batch. If we can't
                // fit a full batch into max bytes, then, then we will be able to cache the size of
                // the batch that will be read in the follow on fetch request.
                nextBatchSize = batchSize;

                final int remaining = batchSize - HEADER_SIZE_UP_TO_MAGIC;
                if (remaining <= buffer.remaining() && Utils.readBytes(inputStream, buffer, remaining) >= remaining) {
                    finalBatchStartPosition = positionCheckpoint;
                    // we fully read the batch, so we do not need to know this batch size for the next fetch
                    nextBatchSize = null;
                } else {
                    buffer.position(positionCheckpoint);
                    break;
                }
            } catch (IOException | IndexOutOfBoundsException ignored) {
                buffer.position(positionCheckpoint);
                break;
            }
        }
    }

    private static long nextOffset(int finalBatchStartPosition, ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(finalBatchStartPosition);
        final ByteBuffer batchBuffer = duplicate.slice();

        final byte magic = batchBuffer.get(MAGIC_OFFSET);
        final RecordBatch batch;
        if (magic < RecordBatch.MAGIC_VALUE_V2)
            batch = new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(batchBuffer);
        else
            batch = new DefaultRecordBatch(batchBuffer);

        return batch.nextOffset();
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
    static RecordBatch readBatch(InputStream inputStream) throws IOException {
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
}
