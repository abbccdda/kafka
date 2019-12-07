/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.tier.fetcher.offsetcache.FetchOffsetMetadata;
import org.apache.kafka.common.record.AbstractLegacyRecordBatch;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.kafka.common.record.DefaultRecordBatch.PARTITION_LEADER_EPOCH_LENGTH;
import static org.apache.kafka.common.record.RecordBatch.CURRENT_MAGIC_VALUE;
import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.MAGIC_LENGTH;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

public class TierSegmentReader {
    private static final Logger log = LoggerFactory.getLogger(TierSegmentReader.class);
    private final String logPrefix;

    public TierSegmentReader(String logPrefix) {
        this.logPrefix = logPrefix;
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
    public RecordsAndNextBatchMetadata readRecords(CancellationContext cancellationContext,
                                                   InputStream inputStream,
                                                   int maxBytes,
                                                   long targetOffset,
                                                   int startBytePosition,
                                                   int segmentSize) throws IOException {
        BatchAndReadState firstBatchAndState = readFirstBatch(cancellationContext, inputStream, targetOffset, segmentSize);
        if (firstBatchAndState == null)
            return new RecordsAndNextBatchMetadata(MemoryRecords.EMPTY, null);

        // insert the first batch, ensuring the buffer size to at least as big as the first
        // batch, even if this exceeds maxBytes
        RecordBatch firstBatch = firstBatchAndState.recordBatch;
        int firstBatchSize = firstBatch.sizeInBytes();
        int totalRequestBytes = Math.max(firstBatchSize, maxBytes);
        ByteBuffer buffer = ByteBuffer.allocate(totalRequestBytes);
        firstBatch.writeTo(buffer);

        ReadState firstBatchState = firstBatchAndState.readState;

        // Read all subsequent batches, if any
        ReadState subsequentBatchState = readInto(cancellationContext, inputStream, buffer, segmentSize);

        NextOffsetAndBatchMetadata nextOffsetAndBatchMetadata;
        if (subsequentBatchState.totalBytesRead > 0) {
           ReadState mergedState = new ReadState(firstBatchState.totalBytesRead + subsequentBatchState.totalBytesRead,
                   subsequentBatchState.lastBatchStartPosition,
                   subsequentBatchState.nextBatchSize,
                   subsequentBatchState.safeToReadMore);
           nextOffsetAndBatchMetadata = determineNextFetchMetadata(buffer, inputStream, mergedState, startBytePosition, segmentSize);
        } else {
           ReadState mergedState = new ReadState(firstBatchState.totalBytesRead,
                   firstBatchState.lastBatchStartPosition,
                   firstBatchState.nextBatchSize,
                   firstBatchState.safeToReadMore && subsequentBatchState.safeToReadMore);
           nextOffsetAndBatchMetadata = determineNextFetchMetadata(buffer, inputStream, mergedState, startBytePosition, segmentSize);
        }

        buffer.flip();
        return new RecordsAndNextBatchMetadata(new MemoryRecords(buffer), nextOffsetAndBatchMetadata);
    }

    /**
     * Read the supplied input stream to find the first offset with a timestamp >= targetTimestamp
     *
     * @param cancellationContext cancellation context to allow reads of InputStream to be aborted
     * @param inputStream         InputStream for the tiered segment
     * @param targetTimestamp     target timestamp to lookup the offset for
     * @param segmentSize         total size of the segment we are reading
     * @return Optional<Long> containing the offset for the supplied target timestamp
     * @throws IOException
     */
    public Optional<Long> offsetForTimestamp(CancellationContext cancellationContext,
                                             InputStream inputStream,
                                             long targetTimestamp,
                                             int segmentSize) throws IOException {
        while (!cancellationContext.isCancelled()) {
            final RecordBatch recordBatch = readBatch(inputStream, segmentSize);
            if (recordBatch.maxTimestamp() >= targetTimestamp) {
                for (final Record record : recordBatch) {
                    if (record.timestamp() >= targetTimestamp)
                        return Optional.of(record.offset());
                }
            }
        }
        return Optional.empty();
    }

    private BatchAndReadState readFirstBatch(CancellationContext cancellationContext,
                                             InputStream inputStream,
                                             long targetOffset,
                                             int segmentSize) throws IOException {
        RecordBatch firstBatch = null;
        int totalBytesRead = 0;

        while (!cancellationContext.isCancelled()) {
            RecordBatch recordBatch = readBatch(inputStream, segmentSize);
            totalBytesRead += recordBatch.sizeInBytes();

            if (recordBatch.baseOffset() <= targetOffset && recordBatch.lastOffset() >= targetOffset) {
                firstBatch = recordBatch;
                break;
            }
        }

        if (firstBatch != null) {
            ReadState readState = new ReadState(totalBytesRead, 0, OptionalInt.empty(), true);
            log.debug("{} completed reading first batch: {}", logPrefix, readState);
            return new BatchAndReadState(firstBatch, readState);
        }

        // We could be here in couple of scenarios:
        // 1. The context was cancelled, forcing us to terminate the fetch.
        // 2. targetOffset is not present in the segment we are reading.
        log.debug("{} could not read first batch", logPrefix);
        return null;
    }

    // Read additional batches from input stream up to maxBytes
    private ReadState readInto(CancellationContext cancellationContext,
                               InputStream inputStream,
                               ByteBuffer buffer,
                               int segmentSize) {
        int initialPosition = buffer.position();
        int lastBatchStartPosition = initialPosition;
        int totalBytesRead = 0;
        OptionalInt nextBatchSize = OptionalInt.empty();
        boolean readPartialData = false;

        // First, fill required bytes into the buffer
        while (!cancellationContext.isCancelled() && buffer.position() < buffer.limit()) {
            try {
                int headerStartPosition = buffer.position();
                int headerBytesRead = Utils.readBytes(inputStream, buffer, HEADER_SIZE_UP_TO_MAGIC);

                // End the read loop if we are at the end of the stream while reading the header
                if (headerBytesRead < HEADER_SIZE_UP_TO_MAGIC) {
                    readPartialData = true;
                    break;
                }

                int batchSize = readMagicAndBatchSize(buffer, headerStartPosition, segmentSize).batchSize;
                int remainingBytes = batchSize - HEADER_SIZE_UP_TO_MAGIC;
                int payloadBytesRead = Utils.readBytes(inputStream, buffer, remainingBytes);

                // End the read loop if we are at the end of the stream while reading the payload. Opportunistically
                // store the size of the batch we were trying to read, as it would be useful for subsequent fetch.
                if (payloadBytesRead < remainingBytes) {
                    log.debug("{} could not read full batch at end of stream", logPrefix);
                    nextBatchSize = OptionalInt.of(batchSize);
                    readPartialData = true;
                    break;
                }

                lastBatchStartPosition = headerStartPosition;
                totalBytesRead += headerBytesRead + remainingBytes;
            } catch (EOFException e) {
                readPartialData = true;
                log.debug("{} terminating read loop due to EOFException", logPrefix, e);
                break;
            } catch (IOException e) {
                readPartialData = true;
                log.error("{} terminating read loop due to IOException", logPrefix, e);
                break;
            }
        }
        buffer.position(initialPosition + totalBytesRead);

        ReadState state = new ReadState(totalBytesRead, lastBatchStartPosition, nextBatchSize, !readPartialData);
        log.debug("{} completed reading all batches: {}", logPrefix, state);
        return state;
    }

    private NextOffsetAndBatchMetadata determineNextFetchMetadata(ByteBuffer buffer,
                                                                  InputStream inputStream,
                                                                  ReadState readState,
                                                                  int startBytePosition,
                                                                  int segmentSize) throws IOException {
        if (readState.totalBytesRead == 0)
            return null;

        // determine nextBatchSize, if we don't already have one
        OptionalInt nextBatchSize = readState.nextBatchSize;
        if (!nextBatchSize.isPresent() && readState.safeToReadMore) {
            ByteBuffer tmpBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);
            Utils.readFully(inputStream, tmpBuffer);
            if (!tmpBuffer.hasRemaining())
                nextBatchSize = OptionalInt.of(readMagicAndBatchSize(tmpBuffer, 0, segmentSize).batchSize);
        }

        // determine nextOffset
        long nextOffset = nextOffset(readState.lastBatchStartPosition, buffer);

        // determine bytePosition
        int nextOffsetBytePosition = startBytePosition + readState.totalBytesRead;

        if (nextOffsetBytePosition < segmentSize)
            return new NextOffsetAndBatchMetadata(new FetchOffsetMetadata(nextOffsetBytePosition, nextBatchSize), nextOffset);
        return null;
    }

    private long nextOffset(int finalBatchStartPosition, ByteBuffer buffer) {
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

    private MagicAndBatchSizePair readMagicAndBatchSize(ByteBuffer buffer,
                                                        int headerStartPosition,
                                                        int segmentSize) {
        final byte magic = buffer.get(headerStartPosition + MAGIC_OFFSET);
        if (magic > CURRENT_MAGIC_VALUE)
            throw new IllegalStateException(logPrefix + " unknown magic: " + magic);

        final int extraLength = HEADER_SIZE_UP_TO_MAGIC - PARTITION_LEADER_EPOCH_LENGTH - MAGIC_LENGTH;
        int batchSize = buffer.getInt(headerStartPosition + SIZE_OFFSET) + extraLength;

        if (batchSize <= 0 || batchSize > segmentSize)
            throw new IllegalStateException(logPrefix + " illegal batch size: " + batchSize);

        return new MagicAndBatchSizePair(magic, batchSize);
    }

    /**
     * Reads one full batch from an InputStream. This method allocates twice per invocation,
     * once to read the header and again to allocate enough space to store the record batch
     * corresponding to the header.
     * <p>
     * Throws EOFException if either the header or full record batch cannot be read.
     * Visible for testing.
     */
    public RecordBatch readBatch(InputStream inputStream, int segmentSize) throws IOException {
        final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);
        final int bytesRead = Utils.readBytes(inputStream, logHeaderBuffer, HEADER_SIZE_UP_TO_MAGIC);
        if (bytesRead < HEADER_SIZE_UP_TO_MAGIC)
            throw new EOFException("Could not read HEADER_SIZE_UP_TO_MAGIC from InputStream");

        logHeaderBuffer.rewind();

        final MagicAndBatchSizePair magicAndBatchSizePair = readMagicAndBatchSize(logHeaderBuffer, 0, segmentSize);
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
        private final byte magic;
        private final int batchSize;

        MagicAndBatchSizePair(byte magic, int batchSize) {
            this.magic = magic;
            this.batchSize = batchSize;
        }
    }

    public static class RecordsAndNextBatchMetadata {
        final MemoryRecords records;
        final NextOffsetAndBatchMetadata nextOffsetAndBatchMetadata;

        public RecordsAndNextBatchMetadata(MemoryRecords records, NextOffsetAndBatchMetadata nextOffsetAndBatchMetadata) {
            this.records = records;
            this.nextOffsetAndBatchMetadata = nextOffsetAndBatchMetadata;
        }
    }

    public static class NextOffsetAndBatchMetadata {
        final FetchOffsetMetadata nextBatchMetadata;
        final long nextOffset;

        public NextOffsetAndBatchMetadata(FetchOffsetMetadata nextBatchMetadata, long nextOffset) {
            this.nextBatchMetadata = nextBatchMetadata;
            this.nextOffset = nextOffset;
        }

        @Override
        public String toString() {
            return "NextOffsetAndBatchMetadata(" +
                    "nextBatchMetadata=" + nextBatchMetadata + ", " +
                    "nextOffset=" + nextOffset +
                    ')';
        }
    }

    private static class BatchAndReadState {
        private final RecordBatch recordBatch;
        private final ReadState readState;

        BatchAndReadState(RecordBatch recordBatch, ReadState readState) {
            this.recordBatch = recordBatch;
            this.readState = readState;
        }
    }

    private static class ReadState {
        final int totalBytesRead;
        final int lastBatchStartPosition;
        final OptionalInt nextBatchSize;
        final boolean safeToReadMore;

        private ReadState(int totalBytesRead,
                          int lastBatchStartPosition,
                          OptionalInt nextBatchSize,
                          boolean safeToReadMore) {
            this.totalBytesRead = totalBytesRead;
            this.lastBatchStartPosition = lastBatchStartPosition;
            this.nextBatchSize = nextBatchSize;
            this.safeToReadMore = safeToReadMore;
        }
    }
}
