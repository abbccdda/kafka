/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.log.Log;
import kafka.log.Log$;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierPartitionStateIllegalListenerException;
import kafka.tier.serdes.ObjectMetadata;
import kafka.tier.serdes.TierPartitionStateHeader;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;

public class FileTierPartitionState implements TierPartitionState, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionState.class);
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final long FILE_OFFSET = 0;
    private static final byte CURRENT_VERSION = 0;

    private final TopicPartition topicPartition;
    private final byte version;
    private final Object lock = new Object();

    private File dir;
    private String path;
    private boolean dirty = false;

    // The current tiering state, including segment metadata and the backing file. All writers must synchronize before
    // mutating any of the components of state. The only operations permitted on the backing file are in-place updates
    // and appends. Any other operations, like physical deletion of metadata, requires a full re-write of the file and
    // creation of a new `State` object. The old file is deleted and the file descriptor is closed asynchronously after
    // a certain timeout to allow for existing readers to complete their operation safely. Readers can thus operate
    // lock-free with one caveat - because `state` could change underneath the reader, all readers must cache the current
    // state once and use it for all subsequent operations, to retrieve segment metadata, read from the underlying file,
    // etc. If the state is re-read, no guarantees are made about the existence of particular segments, their positions
    // in the file relative to the old file, etc.
    private volatile State state;
    private volatile TopicIdPartition topicIdPartition;
    private volatile boolean tieringEnabled;
    private volatile ReplicationMaterializationListener materializationTracker;
    private volatile TierPartitionStatus status = TierPartitionStatus.CLOSED;

    public FileTierPartitionState(File dir, TopicPartition topicPartition, boolean tieringEnabled) throws IOException {
        this(dir, topicPartition, tieringEnabled, CURRENT_VERSION);
    }

    FileTierPartitionState(File dir, TopicPartition topicPartition, boolean tieringEnabled, byte version) throws IOException {
        this.topicPartition = topicPartition;
        this.dir = dir;
        this.path = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
        this.tieringEnabled = tieringEnabled;
        this.state = State.UNINITIALIZED_STATE;
        this.version = version;
        maybeOpenChannel();
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public Optional<TopicIdPartition> topicIdPartition() {
        return Optional.ofNullable(topicIdPartition);
    }

    public void setTopicIdPartition(TopicIdPartition topicIdPartition) throws IOException {
        if (this.topicIdPartition != null && ! this.topicIdPartition.equals(topicIdPartition))
            throw new IllegalStateException("TierPartitionState assigned a different "
                    + "topicIdPartition than already assigned (" + topicIdPartition
                    + " " + this.topicIdPartition);

        this.topicIdPartition = topicIdPartition;
        maybeOpenChannel();
    }

    @Override
    public boolean tieringEnabled() {
        return tieringEnabled;
    }

    @Override
    public void onTieringEnable() throws IOException {
        synchronized (lock) {
            this.tieringEnabled = true;
            maybeOpenChannel();
        }
    }

    @Override
    public Optional<Long> startOffset() {
        Map.Entry<Long, SegmentState> firstEntry = state.segments.firstEntry();
        if (firstEntry != null)
            return Optional.of(firstEntry.getKey());
        return Optional.empty();
    }

    @Override
    public Optional<Long> endOffset() {
        return Optional.ofNullable(state.endOffset);
    }

    @Override
    public Optional<Long> committedEndOffset() {
        return Optional.ofNullable(state.committedEndOffset);
    }

    @Override
    public long totalSize() throws IOException {
        if (!tieringEnabled || !status.isOpen()) {
            throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                    "tieringEnabled: " + tieringEnabled + " file: " + path);
        }

        long size = 0;
        State currentState = state;
        Map.Entry<Long, SegmentState> firstEntry = currentState.segments.firstEntry();

        if (firstEntry != null) {
            FileTierPartitionIterator iterator = iterator(topicIdPartition().get(),
                    currentState.channel,
                    firstEntry.getValue().position);
            while (iterator.hasNext()) {
                TierObjectMetadata metadata = iterator.next();
                size += metadata.size();
            }
        }
        return size;

    }

    @Override
    public void flush() throws IOException {
        synchronized (lock) {
            if (dirty && status.isOpenForWrite()) {
                // flush the file contents
                state.channel.force(true);

                // update the header and flush it
                writeHeader(state.channel, new Header(
                        topicIdPartition.topicId(),
                        version,
                        state.currentEpoch,
                        status));
                state.channel.force(true);

                state.committedEndOffset = state.endOffset;
                dirty = false;
            }
        }
    }

    @Override
    public int tierEpoch() {
        return state.currentEpoch;
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
        synchronized (lock) {
            closeHandlers();
            Files.deleteIfExists(Paths.get(path));
        }
    }

    @Override
    public void updateDir(File dir) {
        synchronized (lock) {
            this.path = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
            this.dir = dir;
        }
    }

    @Override
    public void closeHandlers() throws IOException {
        synchronized (lock) {
            if (status != TierPartitionStatus.CLOSED) {
                try {
                    if (state.channel != null)
                        state.channel.close();
                } finally {
                    state = State.UNINITIALIZED_STATE;
                    if (materializationTracker != null) {
                        materializationTracker.promise.completeExceptionally(
                                new TierPartitionStateIllegalListenerException("Tier partition state for " +
                                        topicPartition + " has been closed."));
                    }
                    status = TierPartitionStatus.CLOSED;
                }
            }
        }
    }

    @Override
    public TierPartitionStatus status() {
        return status;
    }

    @Override
    public void beginCatchup() {
        synchronized (lock) {
            if (!tieringEnabled || !status.isOpen())
                throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                        "tieringEnabled: " + tieringEnabled + " file: " + path);
            setStatus(TierPartitionStatus.CATCHUP);
        }
    }

    @Override
    public void onCatchUpComplete() {
        synchronized (lock) {
            if (!tieringEnabled || !status.isOpen())
                throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                        "tieringEnabled: " + tieringEnabled + " file: " + path);

            setStatus(TierPartitionStatus.ONLINE);
        }
    }

    @Override
    public int numSegments() {
        return segmentOffsets().size();
    }

    @Override
    public Future<TierObjectMetadata> materializationListener(long targetOffset) throws IOException {
        CompletableFuture<TierObjectMetadata> promise = new CompletableFuture<>();
        if (!status.isOpen()) {
            promise.completeExceptionally(new TierPartitionStateIllegalListenerException("Tier partition state for " +
                    topicPartition + " is not open."));
            return promise;
        }

        Optional<TierObjectMetadata> metadata = Optional.empty();
        Optional<Long> uncommittedEndOffset = endOffset();
        if (uncommittedEndOffset.isPresent() && targetOffset <= uncommittedEndOffset.get())
            metadata = metadata(targetOffset);

        if (metadata.isPresent()) {
            // flush to ensure readable TierPartitionState aligns with the local log
            // that will be fetched by the Replica Fetcher. Otherwise we could end up in an unsafe
            // state where the TierPartitionState doesn't align if the broker shuts down
            flush();
            if (metadata.get().endOffset() < targetOffset)
                throw new IllegalStateException("Metadata lookup for offset " + targetOffset +
                        " returned unexpected segment " + metadata);
            // listener is able to fire immediately
            promise.complete(metadata.get());
        } else {
            if (materializationTracker != null)
                materializationTracker.promise.completeExceptionally(new IllegalStateException(
                        "Cancelled materialization tracker, as another materialization tracker has been started."));

            materializationTracker = new ReplicationMaterializationListener(targetOffset, promise);
        }

        return promise;
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            try {
                flush();
            } finally {
                closeHandlers();
            }
        }
    }

    @Override
    public AppendResult append(AbstractTierMetadata entry) throws IOException {
        synchronized (lock) {
            if (!status.isOpenForWrite())
                return AppendResult.ILLEGAL;
            else if (entry instanceof TierTopicInitLeader)
                return append((TierTopicInitLeader) entry);
            else if (entry instanceof TierObjectMetadata)
                return append((TierObjectMetadata) entry);
            else
                throw new RuntimeException(String.format("Unknown TierTopicIndexEntryType %s", entry));
        }
    }

    @Override
    public NavigableSet<Long> segmentOffsets() {
        return state.segments.keySet();
    }

    @Override
    public NavigableSet<Long> segmentOffsets(long from, long to) {
        return Log$.MODULE$.logSegments(state.segments, from, to, lock).keySet();
    }

    @Override
    public Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException {
        State currentState = state;
        Map.Entry<Long, SegmentState> entry = currentState.segments.floorEntry(targetOffset);
        if (entry != null)
            return read(topicIdPartition, currentState, entry.getValue().position);
        else
            return Optional.empty();
    }

    public static Optional<FileTierPartitionIterator> iterator(TopicPartition topicPartition,
                                                               FileChannel channel) throws IOException {
        Optional<Header> headerOpt = readHeader(channel);
        if (!headerOpt.isPresent())
            return Optional.empty();
        return Optional.of(iterator(new TopicIdPartition(topicPartition.topic(),
                        headerOpt.get().topicId(),
                        topicPartition.partition()), channel,
                headerOpt.get().size()));
    }

    // visible for testing
    byte version() {
        return version;
    }

    private static FileTierPartitionIterator iterator(TopicIdPartition topicIdPartition,
                                                      FileChannel channel,
                                                      long position) throws IOException {
        return new FileTierPartitionIterator(topicIdPartition, channel, position);
    }

    private void setStatus(TierPartitionStatus status) {
        this.status = status;
        dirty = true;
    }

    private void maybeOpenChannel() throws IOException {
        if (tieringEnabled && !status.isOpen()) {
            FileChannel channel = getChannelMaybeReinitialize(topicPartition,
                    topicIdPartition,
                    path, version);

            if (channel == null) {
                status = TierPartitionStatus.CLOSED;
                return;
            }
            scanAndInitialize(channel);
        }
    }

    private static Optional<TierObjectMetadata> read(TopicIdPartition topicIdPartition,
                                                     State state,
                                                     long position) throws IOException {
        if (!state.segments.isEmpty()) {
            FileTierPartitionIterator iterator = iterator(topicIdPartition, state.channel, position);
            // The entry at `position` must be known to be fully written to the underlying file
            if (!iterator.hasNext())
                throw new IllegalStateException("Could not read entry at " + position + " for " + "partition " + topicIdPartition);

            return Optional.of(iterator.next());
        }
        return Optional.empty();
    }

    private AppendResult append(TierTopicInitLeader initLeader) {
        if (initLeader.tierEpoch() >= state.currentEpoch) {
            state.currentEpoch = initLeader.tierEpoch();
            dirty = true;
            return AppendResult.ACCEPTED;
        } else {
            return AppendResult.FENCED;
        }
    }

    private AppendResult append(TierObjectMetadata objectMetadata) throws IOException {
        if (objectMetadata.tierEpoch() == tierEpoch()) {
            if (state.endOffset == null || objectMetadata.endOffset() > state.endOffset) {
                ByteBuffer metadataBuffer = objectMetadata.payloadBuffer();
                long byteOffset = appendWithSizePrefix(state.channel, metadataBuffer);
                addSegmentMetadata(objectMetadata.objectMetadata(), byteOffset);
                dirty = true;

                if (status.isOpen() &&
                        materializationTracker != null &&
                        objectMetadata.endOffset() >= materializationTracker.offsetToMaterialize) {
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

    private static long appendWithSizePrefix(FileChannel channel, ByteBuffer metadataBuffer) throws IOException {
        final long byteOffset = channel.position();
        final short sizePrefix = (short) metadataBuffer.remaining();
        final ByteBuffer sizeBuf = ByteBuffer.allocate(ENTRY_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        sizeBuf.putShort(0, sizePrefix);
        Utils.writeFully(channel, sizeBuf);
        Utils.writeFully(channel, metadataBuffer);
        return byteOffset;
    }

    private static void writeHeader(FileChannel channel, Header header) throws IOException {
        short sizePrefix = (short) header.payloadBuffer().remaining();
        ByteBuffer sizeBuf = ByteBuffer.allocate(Header.HEADER_LENGTH_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        sizeBuf.putShort(sizePrefix);
        sizeBuf.flip();
        channel.write(sizeBuf, 0L);
        channel.write(header.payloadBuffer(), Header.HEADER_LENGTH_LENGTH);
    }

    private static Optional<Short> readHeaderSize(FileChannel channel) throws IOException {
        ByteBuffer headerPrefixBuf = ByteBuffer.allocate(Header.HEADER_LENGTH_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        Utils.readFully(channel, headerPrefixBuf, 0);
        headerPrefixBuf.flip();
        if (headerPrefixBuf.limit() == Header.HEADER_LENGTH_LENGTH)
            return Optional.of(headerPrefixBuf.getShort());
        return Optional.empty();
    }

    private static void copy(FileChannel src, FileChannel dest) throws IOException {
        long srcSize = src.size();
        long position = src.position();
        while (position < srcSize)
            position += src.transferTo(position, srcSize - position, dest);
    }

    private static FileChannel getChannelMaybeReinitialize(TopicPartition topicPartition,
                                                           TopicIdPartition topicIdPartition,
                                                           String path,
                                                           byte version) throws IOException {
        FileChannel channel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            Optional<Header> initialHeaderOpt = readHeader(channel);
            if (!initialHeaderOpt.isPresent()) {
                if (topicIdPartition != null) {
                    log.info("Writing new header to tier partition state for {}", topicIdPartition);
                    channel.truncate(0);
                    writeHeader(channel, new Header(topicIdPartition.topicId(),
                            version, -1,
                            TierPartitionStatus.INIT));
                } else {
                    channel.close();
                    return null;
                }
            } else if (initialHeaderOpt.get().header.version() != version) {
                Header initialHeader = initialHeaderOpt.get();
                Path tierFilePath = Paths.get(path);
                Path tmpFilePath = Paths.get(path + ".tmp");

                topicIdPartition = new TopicIdPartition(topicPartition.topic(),
                        initialHeader.topicId(),
                        topicPartition.partition());

                try (FileChannel tmpChannel = FileChannel.open(tmpFilePath, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
                    log.info("Rewriting tier partition state with version {} to {} for {}",
                            initialHeader.header.version(), version, topicIdPartition);
                    Header newHeader = new Header(topicIdPartition.topicId(),
                            version,
                            initialHeader.tierEpoch(),
                            initialHeader.status());
                    writeHeader(tmpChannel, newHeader);
                    tmpChannel.position(newHeader.size());
                    channel.position(initialHeader.size());
                    copy(channel, tmpChannel);
                }
                channel.close();

                Utils.atomicMoveWithFallback(tmpFilePath, tierFilePath);
                channel = FileChannel.open(tierFilePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            }
        } catch (IOException e) {
            channel.close();
            throw e;
        }
        return channel;
    }

    private static Optional<Header> readHeader(FileChannel channel) throws IOException {
        Optional<Short> headerSizeOpt = readHeaderSize(channel);
        if (!headerSizeOpt.isPresent())
            return Optional.empty();
        short headerSize = headerSizeOpt.get();

        ByteBuffer headerBuf = ByteBuffer.allocate(headerSize);
        Utils.readFully(channel, headerBuf, Header.HEADER_LENGTH_LENGTH);
        headerBuf.flip();

        if (headerBuf.limit() != headerSize)
            return Optional.empty();

        return Optional.of(new Header(TierPartitionStateHeader.getRootAsTierPartitionStateHeader(headerBuf)));
    }

    private void scanAndInitialize(FileChannel channel) throws IOException {
        log.debug("scan and truncate TierPartitionState {}", topicPartition);

        state = new State(channel);

        Header header = readHeader(channel).get();

        topicIdPartition = new TopicIdPartition(topicPartition.topic(),
                header.topicId(),
                topicPartition.partition());

        long currentPosition = header.size();

        FileTierPartitionIterator iterator = iterator(topicIdPartition,
                channel,
                currentPosition);
        while (iterator.hasNext()) {
            TierObjectMetadata metadata = iterator.next();
            log.debug("{}: scan reloaded metadata {}", topicPartition, metadata);
            addSegmentMetadata(metadata.objectMetadata(), currentPosition);
            // advance position
            currentPosition = iterator.position();
        }

        if (currentPosition < channel.size()) {
            log.debug("Truncating to {}/{} for partition {}", currentPosition, channel.size(), topicPartition);
            channel.truncate(currentPosition);
        }

        state.committedEndOffset = state.endOffset;
        channel.position(channel.size());
        state.currentEpoch = header.header.tierEpoch();
        status = header.status();
    }

    private void addSegmentMetadata(ObjectMetadata metadata, long byteOffset) {
        // As there may be arbitrary overlap between flushedSegments, it is possible for a new
        // segment to completely overlap a previous segment. We rely on on lookup via the
        // start offset, and if we insert into the lookup map with the raw offset, it is possible
        // for portions of a segment to be unfetchable unless we bound overlapping flushedSegments
        // in the lookup map. e.g. if [100 - 200] is in the map at 100, and we insert [50 - 250]
        // at 50, the portion 201 - 250 will be inaccessible.
        SegmentState segmentState = new SegmentState(metadata.state(), byteOffset);
        state.segments.put(Math.max(endOffset().orElse(-1L) + 1, metadata.startOffset()), segmentState);
        // store end offset for immediate access
        state.endOffset = metadata.startOffset() + metadata.endOffsetDelta();
    }

    private static class ReplicationMaterializationListener {
        final CompletableFuture<TierObjectMetadata> promise;
        final long offsetToMaterialize;

        ReplicationMaterializationListener(long offsetToMaterialize, CompletableFuture<TierObjectMetadata> promise) {
            this.offsetToMaterialize = offsetToMaterialize;
            this.promise = promise;
        }
    }

    private static class Header {
        private static final int HEADER_LENGTH_LENGTH = 2;
        private final TierPartitionStateHeader header;

        Header(TierPartitionStateHeader header) {
            this.header = header;
        }

        Header(UUID topicId, byte version, int tierEpoch,
               TierPartitionStatus status) {
            if (tierEpoch < -1)
                throw new IllegalArgumentException("Illegal tierEpoch " + tierEpoch);

            final FlatBufferBuilder builder = new FlatBufferBuilder(100).forceDefaults(true);
            TierPartitionStateHeader.startTierPartitionStateHeader(builder);
            int topicIdOffset = kafka.tier.serdes.UUID.createUUID(builder,
                    topicId.getMostSignificantBits(),
                    topicId.getLeastSignificantBits());
            TierPartitionStateHeader.addTopicId(builder, topicIdOffset);
            TierPartitionStateHeader.addTierEpoch(builder, tierEpoch);
            TierPartitionStateHeader.addVersion(builder, version);
            TierPartitionStateHeader.addStatus(builder, TierPartitionStatus.toByte(status));
            final int entryId = kafka.tier.serdes.TierPartitionStateHeader.endTierPartitionStateHeader(builder);
            builder.finish(entryId);
            this.header = TierPartitionStateHeader.getRootAsTierPartitionStateHeader(builder.dataBuffer());
        }

        ByteBuffer payloadBuffer() {
            return header.getByteBuffer().duplicate();
        }

        int tierEpoch() {
            return header.tierEpoch();
        }

        UUID topicId() {
            return new UUID(header.topicId().mostSignificantBits(),
                    header.topicId().leastSignificantBits());
        }

        TierPartitionStatus status() {
            return TierPartitionStatus.fromByte(header.status());
        }

        long size() {
            return payloadBuffer().remaining() + HEADER_LENGTH_LENGTH;
        }

        short version() {
            return header.version();
        }

        @Override
        public String toString() {
            return "Header(" +
                    "tierEpoch=" + tierEpoch() + ", " +
                    "status=" + status() + ", " +
                    "version=" + version() +
                    ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(version(), tierEpoch(), status());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Header that = (Header) o;
            return Objects.equals(version(), that.version()) &&
                    Objects.equals(tierEpoch(), that.tierEpoch()) &&
                    Objects.equals(status(), that.status());
        }
    }

    private static class State {
        private final static State UNINITIALIZED_STATE = new State(null);

        private final FileChannel channel;
        private final ConcurrentNavigableMap<Long, SegmentState> segments = new ConcurrentSkipListMap<>();

        private volatile Long endOffset = null;
        private volatile Long committedEndOffset = null;
        private volatile int currentEpoch = -1;

        State(FileChannel channel) {
            this.channel = channel;
        }
    }

    private static class SegmentState {
        private final byte state;
        private final long position;

        SegmentState(byte state, long position) {
            this.state = state;
            this.position = position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || this.getClass() != o.getClass())
                return false;

            SegmentState that = (SegmentState) o;
            return Objects.equals(state, that.state) &&
                    Objects.equals(position, that.position);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, position);
        }
    }
}
