/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordsSend;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Optional;

public class ReclaimableMemoryRecords extends MemoryRecords {
    private enum State {
        EMPTY,
        ACTIVE,
        RELEASED,
    }
    private State state;
    public static final ReclaimableMemoryRecords EMPTY = new ReclaimableMemoryRecords(ByteBuffer.allocate(0), Optional.empty());
    private final MemoryTracker.MemoryLease lease;

    public ReclaimableMemoryRecords(ByteBuffer buffer,
                                    Optional<MemoryTracker.MemoryLease> lease) {
        super(buffer);
        if (lease.isPresent()) {
            this.state = State.ACTIVE;
            this.lease = lease.get();
        } else {
            this.state = State.EMPTY;
            this.lease = null;
        }
    }

    private void throwIfReleased() {
        if (state == State.RELEASED)
            throw new RuntimeException("ReclaimableMemoryRecords used after released");
    }

    @Override
    public int sizeInBytes() {
        throwIfReleased();
        return super.sizeInBytes();
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        throwIfReleased();
        return super.writeTo(channel, position, length);
    }

    @Override
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        throwIfReleased();
        return super.writeFullyTo(channel);
    }

    @Override
    public int validBytes() {
        throwIfReleased();
        return super.validBytes();
    }

    @Override
    public ConvertedRecords<MemoryRecords> downConvert(byte toMagic, long firstOffset, Time time) {
        throwIfReleased();
        return super.downConvert(toMagic, firstOffset, time);
    }

    @Override
    public AbstractIterator<MutableRecordBatch> batchIterator() {
        throwIfReleased();
        return super.batchIterator();
    }

    @Override
    public Integer firstBatchSize() {
        throwIfReleased();
        return super.firstBatchSize();
    }

    @Override
    public FilterResult filterTo(TopicPartition partition, RecordFilter filter, ByteBuffer destinationBuffer, int maxRecordBatchSize, BufferSupplier decompressionBufferSupplier) {
        throwIfReleased();
        return super.filterTo(partition, filter, destinationBuffer, maxRecordBatchSize, decompressionBufferSupplier);
    }

    @Override
    public ByteBuffer buffer() {
        throwIfReleased();
        return super.buffer();
    }

    @Override
    public Iterable<MutableRecordBatch> batches() {
        throwIfReleased();
        return super.batches();
    }

    @Override
    public boolean hasMatchingMagic(byte magic) {
        throwIfReleased();
        return super.hasMatchingMagic(magic);
    }

    @Override
    public boolean hasCompatibleMagic(byte magic) {
        throwIfReleased();
        return super.hasCompatibleMagic(magic);
    }

    @Override
    public RecordBatch firstBatch() {
        throwIfReleased();
        return super.firstBatch();
    }

    @Override
    public Iterable<Record> records() {
        throwIfReleased();
        return super.records();
    }

    @Override
    public RecordsSend toSend(String destination) {
        throwIfReleased();
        return super.toSend(destination);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MemoryRecords self = this;
        MemoryRecords that = (MemoryRecords) o;
        return self.equals(that);
    }

    @Override
    public int hashCode() {
        MemoryRecords self = this;
        return self.hashCode();
    }

    @Override
    public void release() {
        if (state == State.ACTIVE) {
            lease.release();
            state = State.RELEASED;
        }
    }
}
