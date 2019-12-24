/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

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

    // Version 0: Original version
    // Version 1: `endOffset` added to tier partition state header to reduce complexity of determining the correct value when materialized
    // Version 2: `tierStateCommittedOffset` added to tier partition state header. This will be used by metadata integrity validator tool.
    private static final byte CURRENT_VERSION = 2;
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final long FILE_OFFSET = 0;
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionState.class);
    private static final Set<TierObjectMetadata.State> FENCED_STATES = Collections.singleton(TierObjectMetadata.State.SEGMENT_FENCED);

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

    // package-private for testing
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

    public boolean setTopicId(UUID topicId) throws IOException {
        if (topicIdPartition != null) {
           if (!topicIdPartition.topicId().equals(topicId))
               throw new IllegalStateException("Illegal reassignment of topic id. Current: " + topicIdPartition + " Assigned: " + topicId);
           else
               return false;
        }

        topicIdPartition = new TopicIdPartition(topicPartition.topic(), topicId, topicPartition.partition());
        log.info("Setting topicIdPartition {}", topicIdPartition);

        synchronized (lock) {
            maybeOpenChannel();
        }
        return true;
    }

    @Override
    public boolean isTieringEnabled() {
        return tieringEnabled && topicIdPartition != null;
    }

    @Override
    public void enableTierConfig() throws IOException {
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
    public long endOffset() {
        return state.endOffset;
    }

    @Override
    public long committedEndOffset() {
        return state.committedEndOffset();
    }

    @Override
    public long totalSize() {
        return state.validSegmentsSize;
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
                        status,
                        state.endOffset,
                        state.consumptionInfo.localMaterializedOffset));
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
        long uncommittedEndOffset = endOffset();
        if (uncommittedEndOffset != -1L && targetOffset <= uncommittedEndOffset)
            metadata = metadata(targetOffset);

        if (metadata.isPresent()) {
            // flush to ensure readable TierPartitionState aligns with the local log
            // that will be fetched by the Replica Fetcher. Otherwise we could end up in an unsafe
            // state where the TierPartitionState doesn't align if the broker shuts down
            flush();
            if (metadata.get().endOffset() < targetOffset)
                throw new IllegalStateException("Metadata lookup for offset " + targetOffset +
                        " returned unexpected segment " + metadata + " for " + topicIdPartition);
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
                log.info("Tier partition state for {} closed.",
                        topicIdPartition().map(TopicIdPartition::toString).orElse(topicPartition.toString()));
            }
        }
    }

    @Override
    public AppendResult append(AbstractTierMetadata metadata, long tierTopicPartitionOffset) throws IOException {
        synchronized (lock) {
            if (!status.isOpenForWrite()) {
                log.debug("Skipping processing for {} from offset {} as file is not open for write",
                        metadata, tierTopicPartitionOffset);
                return AppendResult.NOT_TIERABLE;
            }

            AppendResult result = appendMetadata(metadata);
            updateLocalMaterializedOffset(tierTopicPartitionOffset);

            log.debug("Processed append for {} with result {} consumed from offset {}",
                    metadata, result, tierTopicPartitionOffset);
            return result;
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
            return readValidObjectMetadata(topicIdPartition, currentState, currentState.position(entry.getValue()), targetOffset);
        else
            return Optional.empty();
    }

    // visible for testing.
    long lastConsumedSrcOffset() {
        return state.consumptionInfo.localMaterializedOffset;
    }

    // visible for testing
    String flushedPath() {
        return flushedFilePath(basePath).toFile().getAbsolutePath();
    }

    public Collection<TierObjectMetadata> fencedSegments() {
        State currentState = state;
        return metadataForStates(topicIdPartition, currentState, FENCED_STATES);
    }

    public static Optional<Header> readHeader(FileChannel channel) throws IOException {
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
        log.info("Status updated to {} for {}", status, topicIdPartition());
    }

    // Caller needs to make sure of thread safety.
    private void updateLocalMaterializedOffset(long offset) {
        // Ignore this duplicate message of previous as the local state has advanced to a future offset, some reasons
        // why a duplicate event can occur are:
        // 1. Transitioning from Catchup mode to Online mode, when catchup consumer surpasses before transition.
        // 2. During recovery, the TierTopicConsumer may start from offset already processed.
        if (state.consumptionInfo.localMaterializedOffset >= offset) {
            log.debug("Ignoring previous TierTopicPartition offset {} for {}", offset, topicIdPartition());
            return;
        }
        state.consumptionInfo.localMaterializedOffset = offset;
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

    /**
     * Reads the first segment object metadata in SEGMENT_UPLOAD_COMPLETE state
     * with offset >= targetOffset. This is intended to be used when a consumer / reader wishes
     * to read records with offset >= targetOffset as it will skip over any segments with
     * endOffset < targetOffset.
     * @param topicIdPartition TopicIdPartition for tier partition state being read
     * @param state write state
     * @param initialBytePosition the initial byte position for the FileTierPartitionIterator
     * @param targetOffset the target offset to be read
     * @return optional TierObjectMetadata for a containing data with offsets >= targetOffset
     * @throws IOException
     */
    private static Optional<TierObjectMetadata> readValidObjectMetadata(TopicIdPartition topicIdPartition,
                                                                        State state,
                                                                        long initialBytePosition,
                                                                        long targetOffset) throws IOException {
        if (!state.validSegments.isEmpty()) {
            FileTierPartitionIterator iterator = iterator(topicIdPartition, state.channel, initialBytePosition);
            // The entry at `position` must be known to be fully written to the underlying file
            if (!iterator.hasNext())
                throw new IllegalStateException("Could not read entry at " + initialBytePosition + " for " + "partition " + topicIdPartition);

            // The first segment at floorOffset may have an endOffset < targetOffset,
            // so we will need to iterate until we find a segment that contains data at an equal or
            // higher offset than the target offset
            while (iterator.hasNext()) {
                TierObjectMetadata metadata = iterator.next();
                if (metadata.endOffset() >= targetOffset && metadata.state().equals(TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE))
                    return Optional.of(metadata);
            }
            return Optional.empty();
        }
        return Optional.empty();
    }

    // Caller needs to make synchronized call.
    private AppendResult appendMetadata(AbstractTierMetadata entry) throws IOException {
        switch (entry.type()) {
            case InitLeader:
                return handleInitLeader((TierTopicInitLeader) entry);

            case SegmentUploadInitiate:
            case SegmentUploadComplete:
            case SegmentDeleteInitiate:
            case SegmentDeleteComplete:
                return maybeTransitionSegment((AbstractTierSegmentMetadata) entry);

            case PartitionDeleteInitiate:
            case PartitionDeleteComplete:
                return AppendResult.ACCEPTED;

            default:
                throw new IllegalStateException("Attempt to append unknown type " + entry.type() + " to " + topicIdPartition);
        }
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
        SegmentState currentState = state.getState(metadata.objectId());

        // Fence transitions that do not belong to the current epoch
        if (metadata.tierEpoch() != state.currentEpoch) {
            log.info("Fenced {} as currentEpoch={} ({})", metadata, state.currentEpoch, topicIdPartition);
            // We do not need to track SegmentUploadInitiate messages that are fenced due to
            // epoch changes for later deletion. The prior leader will also see a fenced
            // transition, and will not stage an upload.
            return AppendResult.FENCED;
        }

        if (currentState != null) {
            if (currentState.state.equals(metadata.state())) {
                // This is a duplicate transition
                log.debug("Accepting duplicate transition for {} ({})", metadata, topicIdPartition);
                return AppendResult.ACCEPTED;
            } else if (!currentState.state.canTransitionTo(metadata.state())) {
                // We have seen this transition before so fence it. This can only happen in couple of scenarios:
                // 1. We are reprocessing messages after an unclean shutdown.
                // 2. We are reprocessing messages after catchup -> online transition, as the primary consumer could be
                //    lagging the catchup consumer before the switch happened.
                // 3. The producer retried a message that was successfully written but was not
                //    acked. Retries will be fenced knowing that:
                //       a) any future completed by the TierTopicManager will have been completed correctly by the
                //          previous copy of this message.
                //       b) This fencing will not be problematic to the archiver due to 3(a)
                //          completing the materialization correctly.
                // We deal with this here for now but in future, we should be able to make this better and assert stronger
                // guarantees by storing the last materialized offset in the tier partition state file and checking if we
                // are reprocessing materialized offsets.
                log.info("Fencing already processed transition for {} with currentState={} ({})", metadata, currentState, topicIdPartition);
                return AppendResult.FENCED;
            }
        } else {
            // If state for this object does not exist, then this must be uploadInitiate
            if (metadata.state() != TierObjectMetadata.State.SEGMENT_UPLOAD_INITIATE)
                throw new IllegalStateException("Cannot complete transition for non-existent segment " + metadata + " for " + topicIdPartition);
        }

        // If we are here, we know this transition is valid: it takes us to the next valid state,
        // it is for the current epoch, and is not a duplicate
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
                throw new IllegalStateException("Unexpected state " + metadata.state() + " for " + topicIdPartition);
        }
    }

    private TierObjectMetadata updateState(UUID objectId, TierObjectMetadata.State newState) throws IOException {
        SegmentState currentState = state.getState(objectId);
        if (currentState == null)
            throw new IllegalStateException("No metadata found for " + objectId + " in " + topicIdPartition);

        TierObjectMetadata metadata = iterator(topicIdPartition, state.channel, currentState.position).next();

        if (!objectId.equals(metadata.objectId()))
            throw new IllegalStateException("id mismatch. Expected: " + objectId + " Got: " + metadata.objectId() + " Partition: " + topicIdPartition);

        metadata.mutateState(newState);
        Utils.writeFully(state.channel, currentState.position + ENTRY_LENGTH_SIZE, metadata.payloadBuffer());
        addSegmentMetadata(metadata, currentState.position);
        dirty = true;
        return metadata;
    }

    private void fenceSegment(TierObjectMetadata metadata) throws IOException {
        updateState(metadata.objectId(), TierObjectMetadata.State.SEGMENT_FENCED);
        if (uploadInProgress != null && uploadInProgress.objectId().equals(metadata.objectId()))
            uploadInProgress = null;
    }

    private AppendResult handleUploadInitiate(TierSegmentUploadInitiate uploadInitiate) throws IOException {
        TierObjectMetadata metadata = new TierObjectMetadata(uploadInitiate);

        if (metadata.endOffset() > state.endOffset) {
            // This is the next in line valid segment to upload belonging to this epoch. Fence any in-progress upload.
            if (uploadInProgress != null)
                fenceSegment(uploadInProgress);

            ByteBuffer metadataBuffer = metadata.payloadBuffer();
            long byteOffset = appendWithSizePrefix(state.channel, metadataBuffer);
            addSegmentMetadata(metadata, byteOffset);
            dirty = true;
            return AppendResult.ACCEPTED;
        }

        // This attempt to upload a segment must be fenced as it covers an offset range that has already been uploaded successfully.
        log.info("Fencing uploadInitiate for {}. currentEndOffset={} currentEpoch={}. ({})",
                metadata, state.endOffset, state.currentEpoch, topicIdPartition);
        return AppendResult.FENCED;
    }

    private AppendResult handleUploadComplete(TierSegmentUploadComplete uploadComplete) throws IOException {
        if (!uploadInProgress.objectId().equals(uploadComplete.objectId()))
            throw new IllegalStateException("Expected " + uploadInProgress.objectId() + " to be in-progress " +
                    "but got " + uploadComplete.objectId() + " for partition " + topicIdPartition);

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
                            TierPartitionStatus.INIT,
                            -1L,
                            -1L));
                } else {
                    channel.close();
                    return null;
                }
            } else if (initialHeaderOpt.get().version() != version) {
                Header initialHeader = initialHeaderOpt.get();
                Path tmpFilePath = tmpFilePath(basePath);

                topicIdPartition = new TopicIdPartition(topicPartition.topic(),
                        initialHeader.topicId(),
                        topicPartition.partition());

                try (FileChannel tmpChannel = FileChannel.open(tmpFilePath, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
                    log.info("Rewriting tier partition state with version {} to {} for {}",
                            initialHeader.version(), version, topicIdPartition);

                    Header newHeader = new Header(topicIdPartition.topicId(),
                            version,
                            initialHeader.tierEpoch(),
                            initialHeader.status(),
                            initialHeader.endOffset(),
                            initialHeader.localMaterializedOffset());
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
                    currentPosition + " size: " + channel.size() + " for partition " + topicIdPartition);

        if (header.endOffset() != -1 && state.endOffset != header.endOffset()) {
            log.info("File header endOffset does not match the materialized endOffset. Setting state endOffset to be " +
                    "equal to header endOffset. Header endOffset: " + header.endOffset() + " materialized state endOffset: " +
                    state.endOffset + " for partition " + topicIdPartition);
            state.endOffset = header.endOffset();
        }

        channel.position(channel.size());
        state.committedEndOffset = state.endOffset;
        state.currentEpoch = header.tierEpoch();
        state.consumptionInfo.localMaterializedOffset = header.localMaterializedOffset();
        status = header.status();

        log.info("Opened tier partition state for {} in status {}. topicIdPartition: {} tierEpoch: {} endOffset: {}",
                topicPartition, status, topicIdPartition(), tierEpoch(), endOffset());
    }



    private void addSegmentMetadata(TierObjectMetadata metadata, long byteOffset) {
        SegmentState segmentState = state.updateAndGetState(byteOffset, metadata);
        switch (metadata.state()) {
            case SEGMENT_UPLOAD_INITIATE:
                if (uploadInProgress != null)
                    throw new IllegalStateException("Unexpected upload in progress " + uploadInProgress +
                            " when appending " + metadata + " to " + topicIdPartition);
                uploadInProgress = metadata.duplicate();
                break;

            case SEGMENT_UPLOAD_COMPLETE:
                state.putValid(segmentState, metadata);
                uploadInProgress = null;
                break;

            case SEGMENT_DELETE_INITIATE:
                state.removeValid(segmentState, metadata);
                break;

            case SEGMENT_DELETE_COMPLETE:
            case SEGMENT_FENCED:
                break;

            default:
                throw new IllegalArgumentException("Unknown state " + metadata + " for " + topicIdPartition);
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

    /**
     * Tracks metadata related to the tier topic partition we are consuming from, to materialize this tier partition
     * state file.
     */
    private static class ConsumptionInfo {
        private volatile long localMaterializedOffset = -1L;  // tracks the last materialized offset
    }

    private static class State {
        private final static State UNINITIALIZED_STATE = new State(null);

        private final FileChannel channel;  // the mutable file channel
        private final ConcurrentNavigableMap<Long, UUID> validSegments = new ConcurrentSkipListMap<>();
        private final ConcurrentNavigableMap<UUID, SegmentState> allSegments = new ConcurrentSkipListMap<>();
        private final ConsumptionInfo consumptionInfo = new ConsumptionInfo();

        private volatile long endOffset = -1L;
        private volatile long committedEndOffset = -1L;
        private volatile int currentEpoch = -1;
        private volatile long validSegmentsSize = 0;

        State(FileChannel channel) {
            this.channel = channel;
        }

        SegmentState updateAndGetState(long byteOffset, TierObjectMetadata metadata) {
            allSegments.putIfAbsent(metadata.objectId(), new SegmentState(startOffsetOfSegment(metadata), byteOffset));
            SegmentState found = allSegments.get(metadata.objectId());
            found.state = metadata.state();
            return found;
        }

        SegmentState getState(UUID objectId) {
            return allSegments.get(objectId);
        }

        void putValid(SegmentState state, TierObjectMetadata metadata) {
            validSegments.put(state.startOffset, metadata.objectId());
            validSegmentsSize += metadata.size();
            endOffset = Math.max(endOffset, metadata.endOffset());
        }

        void removeValid(SegmentState segmentState, TierObjectMetadata metadata) {
            // If the partition state is materialized on broker restart the segment in deleteInitiate will not be
            // included in validSegments, therefore only reduce the validSegmentSize if segment was actually present
            // and removed, this will be the case during runtime.
            UUID toRemove = validSegments.get(segmentState.startOffset);
            if (toRemove != null && toRemove.equals(metadata.objectId())) {
                validSegments.remove(segmentState.startOffset);
                validSegmentsSize -= metadata.size();
            }
        }

        long committedEndOffset() {
            return committedEndOffset;
        }

        long position(UUID objectId) {
            SegmentState state = getState(objectId);
            if (state != null)
                return state.position;
            throw new IllegalStateException("Could not find object " + objectId);
        }

        // As there may be arbitrary overlap between flushedSegments, it is possible for a new
        // segment to completely overlap a previous segment. We rely on on lookup via the
        // start offset, and if we insert into the lookup map with the raw offset, it is possible
        // for portions of a segment to be unfetchable unless we bound overlapping flushedSegments
        // in the lookup map. e.g. if [100 - 200] is in the map at 100, and we insert [50 - 250]
        // at 50, the portion 201 - 250 will be inaccessible.
        private long startOffsetOfSegment(TierObjectMetadata metadata) {
            return Math.max(metadata.baseOffset(), endOffset + 1);
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

    private static class StateCorruptedException extends RetriableException {
        StateCorruptedException(String message) {
            super(message);
        }
    }
}
