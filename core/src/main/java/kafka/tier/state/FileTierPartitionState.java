/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.log.Log;
import kafka.log.Log$;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.FileTierPartitionStateHeader;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierPartitionStateIllegalListenerException;
import kafka.tier.serdes.ObjectMetadata;
import kafka.tier.serdes.TierPartitionStateHeader;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileTierPartitionState implements TierPartitionState, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionState.class);
    private static final int HEADER_PREFIX_SIZE = 2;
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final long FILE_OFFSET = 0;
    private static final byte version = 0;

    private final AtomicInteger currentEpoch = new AtomicInteger(-1);
    private final TopicPartition topicPartition;
    private final ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final boolean readOnly;

    private File dir;
    private String path;
    private ConcurrentNavigableMap<Long, Long> segments = new ConcurrentSkipListMap<>();
    private ConcurrentNavigableMap<Long, Long> unflushedSegments = new ConcurrentSkipListMap<>();

    private volatile FileChannel flushedChannel;
    private volatile FileChannel unflushedChannel;
    private volatile Long uncommittedEndOffset = null;
    private volatile Long endOffset = null;
    private volatile boolean tieringEnabled;
    // Replica Fetcher needs to track materialization progress in order
    // to restore tier state aligned with local data available on the leader
    private volatile ReplicationMaterializationListener materializationTracker = null;
    private volatile TierPartitionStatus status = TierPartitionStatus.CLOSED;
    private volatile boolean dirty = false;

    public FileTierPartitionState(File dir,
                                  TopicPartition topicPartition,
                                  boolean tieringEnabled,
                                  boolean readOnly) throws IOException {
        this.topicPartition = topicPartition;
        this.dir = dir;
        this.path = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
        this.readOnly = readOnly;
        this.tieringEnabled = tieringEnabled;
        if (tieringEnabled)
            openChannels();
        else
            status = TierPartitionStatus.CLOSED;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean tieringEnabled() {
        return tieringEnabled;
    }

    @Override
    public void onTieringEnable() throws IOException {
        flushLock.writeLock().lock();
        try {
            this.tieringEnabled = true;
            if (!status.isOpen())
                openChannels();

        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @Override
    public Optional<Long> startOffset() {
        flushLock.readLock().lock();
        try {
            Map.Entry<Long, Long> firstEntry = segments.firstEntry();
            if (firstEntry != null && firstEntry.getKey() <= endOffset().orElse(-1L)) {
                return Optional.of(firstEntry.getKey());
            }
            return Optional.empty();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    @Override
    public Optional<Long> uncommittedEndOffset() {
        return Optional.ofNullable(uncommittedEndOffset);
    }

    @Override
    public Optional<Long> endOffset() {
        return Optional.ofNullable(endOffset);
    }

    @Override
    public long totalSize() throws IOException {
        if (!tieringEnabled || !status.isOpen()) {
            throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                    "tieringEnabled: " + tieringEnabled + " file: " + path);
        }
        flushLock.readLock().lock();
        try {
            long size = 0;
            Map.Entry<Long, Long> firstEntry = segments.firstEntry();

            if (firstEntry != null) {
                FileTierPartitionIterator iterator = iterator(firstEntry.getValue());
                while (iterator.hasNext()) {
                    TierObjectMetadata metadata = iterator.next();
                    size += metadata.size();
                }
            }
            return size;
        } finally {
            flushLock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (readOnly)
            throw new IllegalStateException("Cannot flush read only TierPartitionState.");

        if (dirty) {
            flushLock.writeLock().lock();
            try {
                if (status.isOpenForWrite()) {
                    writeHeader(unflushedChannel, status, currentEpoch.get());
                    unflushedChannel.force(true);
                    flushedChannel.close();
                    Utils.atomicMoveWithFallback(Paths.get(unflushedPath()), Paths.get(path));
                    flushedChannel = unflushedChannel;
                    segments.putAll(unflushedSegments);
                    setupAppendFile();
                    endOffset = uncommittedEndOffset;
                    dirty = false;
                }
            } finally {
                flushLock.writeLock().unlock();
            }
        }
    }

    @Override
    public int tierEpoch() {
        return currentEpoch.get();
    }

    @Override
    public File dir() {
        return dir;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public void delete() throws IOException {
        if (readOnly)
            throw new IllegalStateException("Cannot delete read only TierPartitionState.");

        flushLock.writeLock().lock();
        try {
            closeHandlers();
            Files.deleteIfExists(Paths.get(path));
            Files.deleteIfExists(Paths.get(unflushedPath()));
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @Override
    public void updateDir(File dir) {
        flushLock.writeLock().lock();
        try {
            this.path = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
            this.dir = dir;
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @Override
    public void closeHandlers() throws IOException {
        if (status != TierPartitionStatus.CLOSED) {
            flushLock.writeLock().lock();
            try {
                try {
                    if (flushedChannel != null)
                        flushedChannel.close();
                    if (unflushedChannel != null)
                        unflushedChannel.close();
                } finally {
                    flushedChannel = null;
                    unflushedChannel = null;
                    segments.clear();
                    unflushedSegments.clear();

                    if (materializationTracker != null) {
                        materializationTracker.promise.completeExceptionally(new TierPartitionStateIllegalListenerException(
                                "Tier partition state for " + topicPartition + " has been closed. "
                                        + "Materialization tracking canceled."));
                    }
                    status = TierPartitionStatus.CLOSED;
                }
            } finally {
                flushLock.writeLock().unlock();
            }
        }
    }

    @Override
    public TierPartitionStatus status() {
        return status;
    }

    @Override
    public void beginCatchup() {
        flushLock.writeLock().lock();
        try {
            if (!tieringEnabled || !status.isOpen())
                throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                        "tieringEnabled: " + tieringEnabled + " file: " + path);

            setStatus(TierPartitionStatus.CATCHUP);
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @Override
    public void onCatchUpComplete() {
        flushLock.writeLock().lock();
        try {
            if (!tieringEnabled || !status.isOpen())
                throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                        "tieringEnabled: " + tieringEnabled + " file: " + path);

            setStatus(TierPartitionStatus.ONLINE);
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @Override
    public int numSegments() {
        return segmentOffsets().size();
    }

    @Override
    public Future<TierObjectMetadata> materializationListener(long targetOffset) throws IOException {
        final CompletableFuture<TierObjectMetadata> promise = new CompletableFuture<>();
        flushLock.readLock().lock();
        Optional<TierObjectMetadata> metadata = Optional.empty();
        try {
            if (!status.isOpen()) {
                promise.completeExceptionally(new TierPartitionStateIllegalListenerException("Tier "
                        + "partition state for " + topicPartition + " is not open. "
                        + "Materialization tracker could not be created."));
                return promise;
            }

            if (uncommittedEndOffset().isPresent() && targetOffset <= uncommittedEndOffset)
                metadata = metadata(targetOffset);
        } finally {
            flushLock.readLock().unlock();
        }

        if (metadata.isPresent()) {
            // flush to ensure readable TierPartitionState aligns with the local log
            // that will be fetched by the Replica Fetcher. Otherwise we could end up in an unsafe
            // state where the TierPartitionState doesn't align if the broker shuts down
            flush();
            assert metadata.get().endOffset() >= targetOffset;
            // listener is able to fire immediately
            promise.complete(metadata.get());
        } else {
            if (materializationTracker != null)
                materializationTracker.promise.completeExceptionally(
                        new IllegalStateException("Cancelled materialization tracker, as "
                                + "another materialization tracker has been started."));

            materializationTracker = new ReplicationMaterializationListener(targetOffset, promise);
        }

        return promise;
    }

    @Override
    public void close() throws IOException {
        try {
            if (!readOnly)
                flush();
        } finally {
            closeHandlers();
        }
    }

    public AppendResult append(AbstractTierMetadata entry) throws IOException {
        if (readOnly)
            throw new IllegalStateException("Cannot append to read only TierPartitionState.");

        if (!status.isOpenForWrite())
            return AppendResult.ILLEGAL;
        else if (entry instanceof TierTopicInitLeader)
            return append((TierTopicInitLeader) entry);
        else if (entry instanceof TierObjectMetadata)
            return append((TierObjectMetadata) entry);
        else
            throw new RuntimeException(String.format("Unknown TierTopicIndexEntryType %s", entry));
    }

    @Override
    public NavigableSet<Long> segmentOffsets() {
        flushLock.readLock().lock();
        try {
            return segments.keySet();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    @Override
    public NavigableSet<Long> segmentOffsets(long from, long to) {
        flushLock.readLock().lock();
        try {
            return Log$.MODULE$.logSegments(segments, from, to).keySet();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    @Override
    public Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException,
            IllegalStateException {
        flushLock.readLock().lock();
        try {
            Map.Entry<Long, Long> entry = segments.floorEntry(targetOffset);
            if (entry != null)
                return read(entry.getValue());
            else
                return Optional.empty();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public FileTierPartitionIterator iterator(long position) throws IOException {
        return new FileTierPartitionIterator(topicPartition, flushedChannel, position);
    }

    public FileTierPartitionIterator iterator() throws IOException {
        return new FileTierPartitionIterator(topicPartition, flushedChannel,
                segments.firstEntry().getValue());
    }

    private void setStatus(TierPartitionStatus status) {
        this.status = status;
        dirty = true;
    }

    private String unflushedPath() {
        return path + ".tmp";
    }

    private static FileChannel open(String path) throws IOException {
        return FileChannel.open(Paths.get(path), StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private void setupAppendFile() throws IOException, IllegalStateException {
        if (readOnly)
            throw new IllegalStateException("Cannot setup append file for read only "
                    + "TierPartitionState.");

        unflushedChannel = FileChannel.open(Paths.get(unflushedPath()),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE);

        // write current header to new append file
        final int written = writeHeader(unflushedChannel, status, currentEpoch.get());
        unflushedChannel.position(written);

        final short flushedHeaderSize = readHeaderSize(flushedChannel);
        if (flushedHeaderSize < 0)
            throw new IllegalStateException("Header size read from flushed TierPartitionState was"
                    + " invalid during initialization of append file.");

        final long flushedSize = flushedChannel.size();
        // start transfer after flushed channel's header
        long readPosition = HEADER_PREFIX_SIZE + flushedHeaderSize;
        while (readPosition < flushedSize) {
            long transferred = flushedChannel.transferTo(readPosition,
                    flushedSize - readPosition,
                    unflushedChannel);
            readPosition += transferred;
        }
    }

    private void openChannels() throws IOException {
        if (tieringEnabled && !status.isOpen()) {
            flushedChannel = open(path);
            scanOrInitialize();
            if (!readOnly) {
                setupAppendFile();
            }
        }
    }

    private Optional<TierObjectMetadata> read(long position) throws IOException, IllegalStateException {
        flushLock.readLock().lock();
        try {
            if (!segments.isEmpty()) {
                FileTierPartitionIterator iterator = iterator(position);
                // The entry at `position` must be known to be fully written to the underlying file
                if (!iterator.hasNext())
                    throw new IllegalStateException("Could not read entry at " + position + " for " + "partition " + topicPartition);

                return Optional.of(iterator.next());
            }
            return Optional.empty();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    private AppendResult append(TierTopicInitLeader initLeader) {
        int epoch = currentEpoch.get();
        if (initLeader.tierEpoch() >= epoch) {
            try {
                flushLock.readLock().lock();
                currentEpoch.set(initLeader.tierEpoch());
                dirty = true;
            } finally {
                flushLock.readLock().unlock();
            }
            return AppendResult.ACCEPTED;
        } else {
            return AppendResult.FENCED;
        }
    }

    private AppendResult append(TierObjectMetadata objectMetadata) throws IOException {
        if (objectMetadata.tierEpoch() == tierEpoch()) {
            if (uncommittedEndOffset == null || objectMetadata.endOffset() > uncommittedEndOffset) {
                final ByteBuffer metadataBuffer = objectMetadata.payloadBuffer();
                flushLock.readLock().lock();
                try {
                    final long byteOffset = appendWithSizePrefix(unflushedChannel, metadataBuffer);
                    addSegment(unflushedSegments, objectMetadata.objectMetadata(), byteOffset);
                    dirty = true;
                } finally {
                    flushLock.readLock().unlock();
                }
                if (status.isOpen()
                        && materializationTracker != null
                        && objectMetadata.endOffset() >= materializationTracker.offsetToMaterialize) {
                    // manually flush to ensure readable TierPartitionState aligns with the local
                    // log that will be fetched by the Replica Fetcher. Otherwise we could end up in an unsafe
                    // state where the TierPartitionState doesn't align if the broker shuts down
                    flush();
                    materializationTracker.promise.complete(objectMetadata);
                    materializationTracker = null;
                }

                return AppendResult.ACCEPTED;
            }
        }
        return AppendResult.FENCED;
    }

    private static long appendWithSizePrefix(FileChannel channel,
                                             ByteBuffer metadataBuffer)
            throws IOException {
        final long byteOffset = channel.position();
        final short sizePrefix = (short) metadataBuffer.remaining();
        final ByteBuffer sizeBuf = ByteBuffer.allocate(ENTRY_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        sizeBuf.putShort(0, sizePrefix);
        Utils.writeFully(channel, sizeBuf);
        Utils.writeFully(channel, metadataBuffer);
        return byteOffset;
    }

    private static int writeHeader(FileChannel channel, TierPartitionStatus status,
                                   Integer epoch) throws IOException {
        FileTierPartitionStateHeader header =
                new FileTierPartitionStateHeader(version, epoch, status);
        final short sizePrefix = (short) header.payloadBuffer().remaining();
        final ByteBuffer sizeBuf = ByteBuffer.allocate(HEADER_PREFIX_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        sizeBuf.putShort(sizePrefix);
        sizeBuf.flip();
        channel.write(sizeBuf, 0L);
        channel.write(header.payloadBuffer(), HEADER_PREFIX_SIZE);

        return HEADER_PREFIX_SIZE + sizePrefix;
    }

    private static short readHeaderSize(FileChannel channel) throws IOException {
        ByteBuffer headerPrefixBuf = ByteBuffer.allocate(HEADER_PREFIX_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        Utils.readFully(channel, headerPrefixBuf, 0);
        headerPrefixBuf.flip();
        if (headerPrefixBuf.limit() == HEADER_PREFIX_SIZE)
            return headerPrefixBuf.getShort();
        else
            return -1;
    }

    private void initializeTierPartitionStateFile() throws IOException {
        if (readOnly)
            throw new IllegalStateException("Empty TierPartitionState file found.");

        flushedChannel.truncate(0);
        status = TierPartitionStatus.INIT;
        currentEpoch.set(-1);
        writeHeader(flushedChannel, status, currentEpoch.get());
        flushedChannel.position(flushedChannel.size());
    }

    private long readHeader() throws IOException, IllegalStateException {
        long currentPosition = 0;

        short headerSize = readHeaderSize(flushedChannel);
        if (headerSize == -1)
            return -1;

        currentPosition += HEADER_PREFIX_SIZE;

        final ByteBuffer headerBuf = ByteBuffer.allocate(headerSize);
        Utils.readFully(flushedChannel, headerBuf, currentPosition);
        headerBuf.flip();

        if (headerBuf.limit() != headerSize)
            return -1;

        currentPosition += headerBuf.limit();

        final FileTierPartitionStateHeader header =
                new FileTierPartitionStateHeader(TierPartitionStateHeader.getRootAsTierPartitionStateHeader(headerBuf));

        status = header.status();
        currentEpoch.set(header.tierEpoch());

        return currentPosition;
    }

    private void scanOrInitialize() throws IOException, IllegalStateException {
        log.debug("scan and truncate TierPartitionState {}", topicPartition);
        final long afterHeaderPosition = readHeader();
        if (afterHeaderPosition == -1) {
            initializeTierPartitionStateFile();
            return;
        }

        try {
            long currentPosition = afterHeaderPosition;
            FileTierPartitionIterator iterator = iterator(currentPosition);

            while (iterator.hasNext()) {
                final TierObjectMetadata metadata = iterator.next();
                log.debug("{}: scan reloaded metadata {}", topicPartition, metadata);
                addSegment(segments, metadata.objectMetadata(), currentPosition);
                // advance position
                currentPosition = iterator.position();
            }

            if (currentPosition < flushedChannel.size()) {
                throw new IOException("TierPartitionState was not able to be fully read.");
            }

            endOffset = uncommittedEndOffset;
            flushedChannel.position(flushedChannel.size());
        } catch (IOException | KafkaStorageException e) {
            log.error("error encountered scanning and truncating TierPartitionState {}. "
                    + "Initializing TierPartitionState file to empty state.", topicPartition, e);
            initializeTierPartitionStateFile();
        }
    }

    private void addSegment(ConcurrentNavigableMap<Long, Long> segmentPositions,
                            ObjectMetadata metadata,
                            long byteOffset) {
        // As there may be arbitrary overlap between segments, it is possible for a new
        // segment to completely overlap a previous segment. We rely on on lookup via the
        // start offset, and if we insert into the lookup map with the raw offset, it is possible
        // for portions of a segment to be unfetchable unless we bound overlapping segments
        // in the lookup map. e.g. if [100 - 200] is in the map at 100, and we insert [50 - 250]
        // at 50, the portion 201 - 250 will be inaccessible.
        segmentPositions.put(Math.max(uncommittedEndOffset().orElse(-1L) + 1, metadata.startOffset()),
                byteOffset);
        // store end offset for immediate access
        uncommittedEndOffset = metadata.startOffset() + metadata.endOffsetDelta();
    }

    private static class ReplicationMaterializationListener {
        final CompletableFuture<TierObjectMetadata> promise;
        final long offsetToMaterialize;

        ReplicationMaterializationListener(long offsetToMaterialize,
                                           CompletableFuture<TierObjectMetadata> promise) {
            this.offsetToMaterialize = offsetToMaterialize;
            this.promise = promise;
        }
    }
}
