/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.log.Log;
import kafka.log.Log$;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.AbstractTierSegmentMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierSegmentDeleteComplete;
import kafka.tier.domain.TierSegmentDeleteInitiate;
import kafka.tier.domain.TierSegmentUploadComplete;
import kafka.tier.domain.TierSegmentUploadInitiate;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierPartitionStateIllegalListenerException;
import kafka.tier.serdes.TierPartitionStateHeader;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.RetriableException;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class FileTierPartitionState implements TierPartitionState, AutoCloseable {
    private enum StateFileType {
        /**
         * Persistent file containing tier partition state. This file is atomically updated on flush. On startup,
         * this file is considered the source-of-truth and reflects the state as of the last successful flush.
         */
        FLUSHED(""),

        /**
         * Mutable file used by writers and readers. On flush, contents in this file are atomically moved to the flushed
         * file. This is achieved by first copying the bytes out to a temporary file and then moving the temporary file
         * to the flushed file atomically. Readers could continue reading the mutable file lock-free because of this
         * out-of-place flush mechanism. This file is overwritten with the flushed file contents on startup.
         */
        MUTABLE(".mutable"),

        /**
         * Temporary file used to overwrite file contents and to move the mutable file contents to the flushed file.
         */
        TEMPORARY(".tmp");

        private String suffix;

        StateFileType(String suffix) {
            this.suffix = suffix;
        }

        public Path filePath(String basePath) {
            return Paths.get(basePath + suffix);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionState.class);
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final long FILE_OFFSET = 0;
    private static final byte CURRENT_VERSION = 0;

    private final TopicPartition topicPartition;
    private final byte version;
    private final Object lock = new Object();

    private File dir;
    private String basePath;
    private TierObjectMetadata uploadInProgress;  // cached for quick lookup
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
        this.basePath = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
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
        if (this.topicIdPartition != null && !this.topicIdPartition.equals(topicIdPartition))
            throw new IllegalStateException("TierPartitionState assigned a different "
                    + "topicIdPartition than already assigned (" + topicIdPartition
                    + " " + this.topicIdPartition);
        this.topicIdPartition = topicIdPartition;
        synchronized (lock) {
            maybeOpenChannel();
        }
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
        Map.Entry<Long, UUID> firstEntry = state.validSegments.firstEntry();
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
                    "tieringEnabled: " + tieringEnabled + " basePath: " + basePath);
        }

        long size = 0;
        State currentState = state;
        Map.Entry<Long, UUID> firstEntry = currentState.validSegments.firstEntry();

        if (firstEntry != null) {
            FileTierPartitionIterator iterator = iterator(topicIdPartition().get(),
                    currentState.channel,
                    currentState.position(firstEntry.getValue()));
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
                // update the header and flush file contents
                writeHeader(state.channel, new Header(
                        topicIdPartition.topicId(),
                        version,
                        state.currentEpoch,
                        status));
                state.channel.force(true);

                // move file contents to the flushed file
                Files.copy(mutableFilePath(basePath), tmpFilePath(basePath), StandardCopyOption.REPLACE_EXISTING);
                Utils.atomicMoveWithFallback(tmpFilePath(basePath), flushedFilePath(basePath));

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
    public void delete() throws IOException {
        synchronized (lock) {
            closeHandlers();
            for (StateFileType type : StateFileType.values())
                Files.deleteIfExists(type.filePath(basePath));
        }
    }

    @Override
    public void updateDir(File dir) {
        synchronized (lock) {
            this.basePath = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
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
                    uploadInProgress = null;
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
                        "tieringEnabled: " + tieringEnabled + " basePath: " + basePath);
            setStatus(TierPartitionStatus.CATCHUP);
        }
    }

    @Override
    public void onCatchUpComplete() {
        synchronized (lock) {
            if (!tieringEnabled || !status.isOpen())
                throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                        "tieringEnabled: " + tieringEnabled + " basePath: " + basePath);

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

            switch (entry.type()) {
                case InitLeader:
                    return handleInitLeader((TierTopicInitLeader) entry);

                case SegmentUploadInitiate:
                case SegmentUploadComplete:
                case SegmentDeleteInitiate:
                case SegmentDeleteComplete:
                    return maybeTransitionSegment((AbstractTierSegmentMetadata) entry);

                default:
                    throw new IllegalStateException("Unknown type " + entry.type());
            }
        }
    }

    @Override
    public NavigableSet<Long> segmentOffsets() {
        return state.validSegments.keySet();
    }

    @Override
    public NavigableSet<Long> segmentOffsets(long from, long to) {
        return Log$.MODULE$.logSegments(state.validSegments, from, to, lock).keySet();
    }

    @Override
    public Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException {
        State currentState = state;
        Map.Entry<Long, UUID> entry = currentState.validSegments.floorEntry(targetOffset);
        if (entry != null)
            return read(topicIdPartition, currentState, currentState.position(entry.getValue()));
        else
            return Optional.empty();
    }

    // visible for testing
    String flushedPath() {
        return flushedFilePath(basePath).toFile().getAbsolutePath();
    }

    public Collection<TierObjectMetadata> fencedSegments() {
        State currentState = state;
        return metadataForStates(topicIdPartition, currentState, Collections.singleton(TierObjectMetadata.State.SEGMENT_FENCED));
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

    private static List<TierObjectMetadata> metadataForStates(TopicIdPartition topicIdPartition,
                                                              State currentState,
                                                              Set<TierObjectMetadata.State> states) {
        return currentState.allSegments.values()
                .stream()
                .filter(segmentState -> states.contains(segmentState.state))
                .map(segmentState -> {
                    try {
                        return iterator(topicIdPartition, currentState.channel, segmentState.position).next();
                    } catch (IOException e) {
                        throw new KafkaStorageException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private void maybeOpenChannel() throws IOException {
        if (tieringEnabled && !status.isOpen()) {
            Path flushedFilePath = flushedFilePath(basePath);
            Path mutableFilePath = mutableFilePath(basePath);

            if (!Files.exists(flushedFilePath))
                Files.createFile(flushedFilePath);
            Files.copy(flushedFilePath, mutableFilePath, StandardCopyOption.REPLACE_EXISTING);

            FileChannel channel = getChannelMaybeReinitialize(topicPartition, topicIdPartition, basePath, version);

            if (channel == null) {
                status = TierPartitionStatus.CLOSED;
                return;
            }

            try {
                scanAndInitialize(channel);
            } catch (StateCorruptedException e) {
                // Reinitialize file in catchup state when we detect corruption
                closeHandlers();
                Files.delete(flushedFilePath);
                maybeOpenChannel();
                beginCatchup();
            }
        }
    }

    private static Optional<TierObjectMetadata> read(TopicIdPartition topicIdPartition,
                                                     State state,
                                                     long position) throws IOException {
        if (!state.validSegments.isEmpty()) {
            FileTierPartitionIterator iterator = iterator(topicIdPartition, state.channel, position);
            // The entry at `position` must be known to be fully written to the underlying file
            if (!iterator.hasNext())
                throw new IllegalStateException("Could not read entry at " + position + " for " + "partition " + topicIdPartition);

            return Optional.of(iterator.next());
        }
        return Optional.empty();
    }

    private AppendResult handleInitLeader(TierTopicInitLeader initLeader) throws IOException {
        // We accept any epoch >= the current one, as there could be duplicate init leader messages for the current
        // epoch.
        if (initLeader.tierEpoch() == state.currentEpoch) {
            return AppendResult.ACCEPTED;
        } else if (initLeader.tierEpoch() > state.currentEpoch) {
            // On leader change, we fence all in-progress uploads and segments that were being deleted
            Set<TierObjectMetadata.State> statesToFence = new HashSet<>(Arrays.asList(
                    TierObjectMetadata.State.SEGMENT_UPLOAD_INITIATE,
                    TierObjectMetadata.State.SEGMENT_DELETE_INITIATE)
            );
            List<TierObjectMetadata> toFence = metadataForStates(topicIdPartition, state, statesToFence);
            for (TierObjectMetadata metadata : toFence)
                fenceSegment(metadata);

            state.currentEpoch = initLeader.tierEpoch();
            dirty = true;
            return AppendResult.ACCEPTED;
        } else {
            return AppendResult.FENCED;
        }
    }

    private AppendResult maybeTransitionSegment(AbstractTierSegmentMetadata metadata) throws IOException {
        SegmentState currentState = state.allSegments.get(metadata.objectId());

        // Fence transition that do not belong to the current epoch
        if (metadata.tierEpoch() != state.currentEpoch) {
            log.info("Fenced {} as currentEpoch={}.", metadata, state.currentEpoch);
            return AppendResult.FENCED;
        }

        if (currentState != null) {
            if (currentState.state.equals(metadata.state())) {
                // This is a duplicate transition
                log.debug("Accepting duplicate transition for {}", metadata);
                return AppendResult.ACCEPTED;
            } else if (!currentState.state.canTransitionTo(metadata.state())) {
                // This transition will not take us to the next valid state
                throw new IllegalStateException("Illegal attempt to transition " + currentState + " to " + metadata);
            }
        } else {
            // If state for this object does not exist, then this must be uploadInitiate
            if (metadata.state() != TierObjectMetadata.State.SEGMENT_UPLOAD_INITIATE)
                throw new IllegalStateException("Cannot complete transition for non-existent segment " + metadata);
        }

        // If we are here, we know this transition is valid: it takes us to the next valid state, it is for the current
        // epoch, and is not a duplicate
        switch (metadata.state()) {
            case SEGMENT_UPLOAD_INITIATE:
                return handleUploadInitiate((TierSegmentUploadInitiate) metadata);
            case SEGMENT_UPLOAD_COMPLETE:
                return handleUploadComplete((TierSegmentUploadComplete) metadata);
            case SEGMENT_DELETE_INITIATE:
                return handleDeleteInitiate((TierSegmentDeleteInitiate) metadata);
            case SEGMENT_DELETE_COMPLETE:
                return handleDeleteComplete((TierSegmentDeleteComplete) metadata);
            default:
                throw new IllegalStateException("Unexpected state " + metadata.state());
        }
    }

    private TierObjectMetadata updateState(UUID objectId, TierObjectMetadata.State newState) throws IOException {
        SegmentState currentState = state.allSegments.get(objectId);
        if (currentState == null)
            throw new IllegalStateException("No metadata found for " + objectId);

        TierObjectMetadata metadata = iterator(topicIdPartition, state.channel, currentState.position).next();

        if (!objectId.equals(metadata.objectId()))
            throw new IllegalStateException("id mismatch. Expected: " + objectId + " Got: " + metadata.objectId());

        metadata.mutateState(newState);
        Utils.writeFully(state.channel, currentState.position + ENTRY_LENGTH_SIZE, metadata.payloadBuffer());
        addSegmentMetadata(metadata, currentState.position);
        dirty = true;
        return metadata;
    }

    private void fenceSegment(TierObjectMetadata metadata) throws IOException {
        updateState(metadata.objectId(), TierObjectMetadata.State.SEGMENT_FENCED);
        if (uploadInProgress.objectId().equals(metadata.objectId()))
            uploadInProgress = null;
    }

    private AppendResult handleUploadInitiate(TierSegmentUploadInitiate uploadInitiate) throws IOException {
        TierObjectMetadata metadata = new TierObjectMetadata(uploadInitiate);

        if (state.endOffset == null || metadata.endOffset() > state.endOffset) {
            // This is the next in line valid segment to upload belonging to this epoch. Fence any in-progress upload.
            if (uploadInProgress != null)
                fenceSegment(uploadInProgress);

            ByteBuffer metadataBuffer = metadata.payloadBuffer();
            long byteOffset = appendWithSizePrefix(state.channel, metadataBuffer);
            addSegmentMetadata(metadata, byteOffset);
            dirty = true;
            return AppendResult.ACCEPTED;
        }

        // This attempt to upload a segment must be fenced as it belongs to a previous epoch or covers an offset
        // range that has already been uploaded successfully.
        log.info("Fencing uploadInitiate for {}. currentEndOffset={} currentEpoch={}.",
                metadata, state.endOffset, state.currentEpoch);
        return AppendResult.FENCED;
    }

    private AppendResult handleUploadComplete(TierSegmentUploadComplete uploadComplete) throws IOException {
        if (!uploadInProgress.objectId().equals(uploadComplete.objectId()))
            throw new IllegalStateException("Expected " + uploadInProgress.objectId() + " to be in-progress but got " + uploadComplete.objectId());

        TierObjectMetadata metadata = updateState(uploadComplete.objectId(), TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE);

        if (materializationTracker != null &&
                metadata.endOffset() >= materializationTracker.offsetToMaterialize) {
            // manually flush to ensure readable TierPartitionState aligns with the local
            // log that will be fetched by the Replica Fetcher. Otherwise we could end up in an unsafe
            // state where the TierPartitionState doesn't align if the broker shuts down
            flush();
            materializationTracker.promise.complete(metadata);
            materializationTracker = null;
        }
        return AppendResult.ACCEPTED;
    }

    private AppendResult handleDeleteInitiate(TierSegmentDeleteInitiate deleteInitiate) throws IOException {
        updateState(deleteInitiate.objectId(), TierObjectMetadata.State.SEGMENT_DELETE_INITIATE);
        return AppendResult.ACCEPTED;
    }

    private AppendResult handleDeleteComplete(TierSegmentDeleteComplete deleteComplete) throws IOException {
        updateState(deleteComplete.objectId(), TierObjectMetadata.State.SEGMENT_DELETE_COMPLETE);
        return AppendResult.ACCEPTED;
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
        Utils.writeFully(channel, 0, sizeBuf);
        Utils.writeFully(channel, Header.HEADER_LENGTH_LENGTH, header.payloadBuffer());
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
                                                           String basePath,
                                                           byte version) throws IOException {
        Path mutableFilePath = mutableFilePath(basePath);
        FileChannel channel = FileChannel.open(mutableFilePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
                Path tmpFilePath = tmpFilePath(basePath);

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

                Utils.atomicMoveWithFallback(tmpFilePath, mutableFilePath);
                channel = FileChannel.open(mutableFilePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
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

    private void scanAndInitialize(FileChannel channel) throws IOException, StateCorruptedException {
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
            addSegmentMetadata(metadata, currentPosition);
            // advance position
            currentPosition = iterator.position();
        }

        if (currentPosition < channel.size())
            throw new StateCorruptedException("Could not read all bytes in file. position: " +
                    currentPosition + " size: " + channel.size());

        state.committedEndOffset = state.endOffset;
        channel.position(channel.size());
        state.currentEpoch = header.header.tierEpoch();
        status = header.status();
    }

    // As there may be arbitrary overlap between flushedSegments, it is possible for a new
    // segment to completely overlap a previous segment. We rely on on lookup via the
    // start offset, and if we insert into the lookup map with the raw offset, it is possible
    // for portions of a segment to be unfetchable unless we bound overlapping flushedSegments
    // in the lookup map. e.g. if [100 - 200] is in the map at 100, and we insert [50 - 250]
    // at 50, the portion 201 - 250 will be inaccessible.
    private long startOffsetOfSegment(TierObjectMetadata metadata) {
        return Math.max(metadata.baseOffset(), endOffset().orElse(-1L) + 1);
    }

    private void addSegmentMetadata(TierObjectMetadata metadata, long byteOffset) {
        state.allSegments.putIfAbsent(metadata.objectId(), new SegmentState(startOffsetOfSegment(metadata), byteOffset));
        SegmentState segmentState = state.allSegments.get(metadata.objectId());
        segmentState.state = metadata.state();
        long startOffset = segmentState.startOffset;

        switch (metadata.state()) {
            case SEGMENT_UPLOAD_INITIATE:
                if (uploadInProgress != null)
                    throw new IllegalStateException("Unexpected upload in progress " + uploadInProgress +
                            " when appending " + metadata);
                uploadInProgress = metadata.duplicate();
                break;

            case SEGMENT_UPLOAD_COMPLETE:
                state.validSegments.put(startOffset, metadata.objectId());
                state.endOffset = metadata.endOffset();  // store end offset for immediate access
                uploadInProgress = null;
                break;

            case SEGMENT_DELETE_INITIATE:
                state.validSegments.remove(startOffset);
                break;

            case SEGMENT_DELETE_COMPLETE:
            case SEGMENT_FENCED:
                break;

            default:
                throw new IllegalArgumentException("Unknown state " + metadata);
        }
    }

    private static Path flushedFilePath(String basePath) {
        return StateFileType.FLUSHED.filePath(basePath);
    }

    private static Path mutableFilePath(String basePath) {
        return StateFileType.MUTABLE.filePath(basePath);
    }

    private static Path tmpFilePath(String basePath) {
        return StateFileType.TEMPORARY.filePath(basePath);
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

        private final FileChannel channel;  // the mutable file channel
        private final ConcurrentNavigableMap<Long, UUID> validSegments = new ConcurrentSkipListMap<>();
        private final ConcurrentNavigableMap<UUID, SegmentState> allSegments = new ConcurrentSkipListMap<>();

        private volatile Long endOffset = null;
        private volatile Long committedEndOffset = null;
        private volatile int currentEpoch = -1;

        State(FileChannel channel) {
            this.channel = channel;
        }

        long position(UUID objectId) {
            SegmentState state = allSegments.get(objectId);
            if (state != null)
                return state.position;
            throw new IllegalStateException("Could not find object " + objectId);
        }
    }

    private static class SegmentState {
        private TierObjectMetadata.State state;
        private final long startOffset;   // start offset of segment; this may be different from the base offset
        private final long position;

        SegmentState(long startOffset, long position) {
            this.startOffset = startOffset;
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
                    Objects.equals(startOffset, that.startOffset) &&
                    Objects.equals(position, that.position);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, startOffset, position);
        }

        @Override
        public String toString() {
            return "SegmentState(" +
                    "state: " + state + ", " +
                    "startOffset: " + startOffset + ", " +
                    "position: " + position +
                    ")";
        }
    }

    private class StateCorruptedException extends RetriableException {
        StateCorruptedException(String message) {
            super(message);
        }
    }
}
