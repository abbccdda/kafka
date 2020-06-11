/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.log.Log;
import kafka.log.Log$;
import kafka.server.LogDirFailureChannel;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.AbstractTierSegmentMetadata;
import kafka.tier.domain.TierPartitionFence;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierPartitionForceRestore;
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
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.CloseableIterator;
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
import java.util.Iterator;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static scala.compat.java8.JFunction.func;

/**
 * Important: this code is performance sensitive. When making potentially performance sensitive
 * code, please run the following under the jmh-benchmarks directory:
 * ./jmh.sh -prof gc org.apache.kafka.jmh.tier.MergedLogTierBenchmark
 */
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
        TEMPORARY(".tmp"),

        /**
         * Error file used to backup state that experienced a materialization error.
         */
        ERROR(".error"),

        /**
         * Overwritten tier partition state after a tier restore
         */
        DISCARDED(".discarded"),

        /**
         * Staged tier partition state for a tier restore
         */
        RECOVER(".recover");

        private String suffix;

        StateFileType(String suffix) {
            this.suffix = suffix;
        }

        public Path filePath(String basePath) {
            return Paths.get(basePath + suffix);
        }
    }

    // Version 0: Initial version
    // Version 1: `endOffset` added to tier partition state header
    // Version 2: `globalMaterializedOffset` and `localMaterializedOffset` added to tier partition state header
    // Version 3: introduced new header status: `TierPartitionStatus.ERROR` as a part of the fencing mechanism
    // Version 4: `globalMaterializedEpoch` and `localMaterializedEpoch` added to tier partition state header
    // Version 5: `errorOffsetAndEpoch` added to tier partition state header
    // Version 6: `restoreOffsetAndEpoch` and start offset added to tier partition state header,
    //             and `stateOffsetAndEpoch` added to segment metadata
    static final byte CURRENT_VERSION = 6;
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final long FILE_OFFSET = 0;
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionState.class);
    private static final Set<TierObjectMetadata.State> FENCED_STATES = Collections.singleton(TierObjectMetadata.State.SEGMENT_FENCED);

    private final TopicPartition topicPartition;
    private final byte version;
    //
    // 1. stateResourceReadWriteLock: This protects readers of the inner state from any internal resources from being closed while used
    //                                concurrently without the stateLock.
    // 2. stateLock: This protects the mutable state in general. Among other things, the mutable state also includes the `State` object
    //                   held in the `FileTierPartitionState.state` handle.
    //                a. Outer point R/W: If the read or write pattern involves doing a point read or write (ex: read or write any 1 volatile attribute)
    //                  outside the `FileTierPartitionState.state` handle, then, this lock can be skipped.
    //                b. Outer multi R/W: If the read or write pattern involves reading or modifying more than 1 volatile attribute at once
    //                  outside the `FileTierPartitionState.state` handle, then, it is advised to take this lock to ensure a consistent
    //                  read behavior from all attributes being modified in a specific context.
    //                c. Inner point R: For point reads within the object held in the `FileTierPartitionState.state` handle, this lock can be skipped
    //                   (but stateHandleReadWriteLock must be held to protect the handle from concurrent modification).
    //                d. Inner W: For mutations within the `FileTierPartitionState.state` handle, this lock should be always taken, and
    //                   `stateHandleReadWriteLock` should be held prior to taking this lock.
    //
    // If the thread safety requirement involves chaining both the locks (like in 2(d) above), then the lock acquisition ordering
    // requirement always should be the following:
    //  - Lock #1: Take stateResourceReadWriteLock (either the read lock or the write lock as needed)
    //  - Lock #2: Take stateLock


    private final ReentrantReadWriteLock stateResourceReadWriteLock = new ReentrantReadWriteLock(); // re-entrancy is not expected
    private final Object stateLock = new Object();

    private final Consumer<IOException> ioExceptionHandler;

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
    private volatile String basePath;
    private volatile File dir;

    public FileTierPartitionState(File dir, LogDirFailureChannel logDirFailureChannel, TopicPartition topicPartition, boolean tieringEnabled) throws IOException {
        this(dir, logDirFailureChannel, topicPartition, tieringEnabled, CURRENT_VERSION);
    }

    // package-private for testing
    FileTierPartitionState(File dir,
                           LogDirFailureChannel logDirFailureChannel,
                           TopicPartition topicPartition,
                           boolean tieringEnabled,
                           byte version) throws IOException {
        this.topicPartition = topicPartition;
        this.dir = dir;
        this.ioExceptionHandler = e -> logDirFailureChannel.maybeAddOfflineLogDir(dir().getParent(),
                        func(() -> "IOException encountered in TierPartitionState at " + dir().getParent()), e);
        this.basePath = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
        this.tieringEnabled = tieringEnabled;
        this.state = State.EMPTY;
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
        maybeOpenChannel();

        return true;
    }

    @Override
    public boolean isTieringEnabled() {
        synchronized (stateLock) {
            return tieringEnabled && topicIdPartition != null;
        }
    }

    @Override
    public void enableTierConfig() throws IOException {
        this.tieringEnabled = true;
        maybeOpenChannel();
    }

    @Override
    public Optional<Long> startOffset() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            // safe to read without the state handle lock as it uses
            // a concurrent collection in a consistent way
            return state.startOffset();
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public long endOffset() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            // safe to read without the state handle lock as it accesses a member volatile
            return state.endOffset();
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public long committedEndOffset() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            // safe to read without the state handle lock as it accesses a member volatile
            return state.committedEndOffset();
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public long totalSize() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            // safe to read without the state handle lock as it accesses a member volatile
            return state.totalSize();
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                state.flush();
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    private static void backupState(TopicIdPartition topicIdPartition,
                                    String basePath,
                                    Path dstPath) throws IOException {
        Path srcPath = mutableFilePath(basePath);
        if (!Files.exists(srcPath))
            return;
        Files.copy(srcPath, tmpFilePath(basePath), StandardCopyOption.REPLACE_EXISTING);
        Utils.atomicMoveWithFallback(tmpFilePath(basePath), dstPath);
        log.info(
            "Backed up mutable file from: {} to: {}, topicIdPartition={}",
            srcPath, dstPath, topicIdPartition);
    }

    @Override
    public int tierEpoch() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            // safe to read without the state handle lock as it accesses a member volatile
            return state.currentEpoch();
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public File dir() {
        return dir;
    }

    @Override
    public void delete() throws IOException {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                closeHandlersImpl();
                for (StateFileType type : StateFileType.values())
                    Files.deleteIfExists(type.filePath(basePath));
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public void updateDir(File dir) {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                this.basePath = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
                this.dir = dir;
                state.updateBasePath(basePath);
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public RestoreResult restoreState(TierPartitionForceRestore metadata,
                                      ByteBuffer targetState,
                                      TierPartitionStatus targetStatus,
                                      OffsetAndEpoch offsetAndEpoch) {
        // take the state handle write lock to prevent any readers from
        // accessing the state while undergoing restore
        stateResourceReadWriteLock.writeLock().lock();
        try {
            // take the global lock second
            synchronized (stateLock) {
                return restoreStateImpl(metadata, targetState, targetStatus, offsetAndEpoch);
            }
        } finally {
            stateResourceReadWriteLock.writeLock().unlock();
        }
    }


    @Override
    public void closeHandlers() throws IOException {
        stateResourceReadWriteLock.writeLock().lock();
        try {
            synchronized (stateLock) {
                closeHandlersImpl();
            }
        } finally {
            stateResourceReadWriteLock.writeLock().unlock();
        }
    }


    @Override
    public TierPartitionStatus status() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            return state.status();
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public long materializationLag() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            if (state.materializationListener != null) {
                return state.materializationListener.offsetToMaterialize() - Math.max(0L, state.endOffset);
            }
            return 0L;
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public void beginCatchup() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                if (!tieringEnabled)
                    throw new IllegalStateException("Illegal state for tier partition. " +
                            "tieringEnabled: " + tieringEnabled + " basePath: " + basePath);
                state.beginCatchup();
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public void onCatchUpComplete() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                if (!tieringEnabled)
                    throw new IllegalStateException("Illegal state for tier partition. " +
                            "tieringEnabled: " + tieringEnabled + " basePath: " + basePath);
                state.onCatchUpComplete();
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public int numSegments(long from, long to) {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                return state.segmentOffsets(from, to).size();
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public int numSegments() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                return state.segmentOffsets().size();
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public Future<TierObjectMetadata> materializeUpto(long targetOffset) throws IOException {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                return state.materializationListener(targetOffset);
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Optional<TierObjectMetadata>> materializeUptoEpoch(int targetEpoch) throws IOException {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                return state.materializeUpto(targetEpoch);
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                try {
                    state.flush();
                } finally {
                    closeHandlersImpl();
                    log.info("Tier partition state for {} closed.",
                            topicIdPartition().map(TopicIdPartition::toString).orElse(topicPartition.toString()));
                }
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    @Override
    public AppendResult append(AbstractTierMetadata metadata, OffsetAndEpoch offsetAndEpoch) {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                return state.appendMetadata(metadata, offsetAndEpoch);
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    public CloseableIterator<TierObjectMetadata> segments() {
        stateResourceReadWriteLock.readLock().lock();
        synchronized (stateLock) {
            return tierObjectMetadataCloseableIterator(state.segments(), stateResourceReadWriteLock);
        }
    }

    public CloseableIterator<TierObjectMetadata> segments(long from, long to) {
        stateResourceReadWriteLock.readLock().lock();
        synchronized (stateLock) {
            return tierObjectMetadataCloseableIterator(state.segments(from, to), stateResourceReadWriteLock);
        }
    }

    @Override
    public Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException {
        stateResourceReadWriteLock.readLock().lock();
        try {
            return state.metadata(targetOffset);
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    // visible for testing.
    public OffsetAndEpoch lastLocalMaterializedSrcOffsetAndEpoch() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            return state.localMaterializedOffsetAndEpoch;
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    // visible for testing.
    OffsetAndEpoch lastFlushedSrcOffsetAndEpoch() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            return state.globalMaterializedOffsetAndEpoch;
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    // visible for testing.
    OffsetAndEpoch restoreOffsetAndEpoch() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            return state.restoreOffsetAndEpoch;
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    // visible for testing.
    OffsetAndEpoch lastFlushedErrorOffsetAndEpoch() {
        return state.errorOffsetAndEpoch;
    }

    // visible for testing
    public String flushedPath() {
        return flushedFilePath(basePath).toFile().getAbsolutePath();
    }

    public Collection<TierObjectMetadata> fencedSegments() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            return state.metadataForStates(FENCED_STATES);
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    public String toString() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                if (tieringEnabled)
                    return "FileTierPartitionState("
                            + "topicIdPartition=" + topicIdPartition
                            + "state=" + state
                            + ")";
                else
                    return "FileTierPartitionState("
                            + "topicIdPartition=" + topicIdPartition
                            + ", tieringEnabled=" + tieringEnabled
                            + ")";
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
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

    // visible for testing
    String basePath() {
        return basePath;
    }

    private static FileTierPartitionIterator iterator(TopicIdPartition topicIdPartition,
                                                      FileChannel channel,
                                                      long position) throws IOException {
        return new FileTierPartitionIterator(topicIdPartition, channel, position);
    }

    private RestoreResult restoreStateImpl(TierPartitionForceRestore metadata,
                                           ByteBuffer targetState,
                                           TierPartitionStatus targetStatus,
                                           OffsetAndEpoch offsetAndEpoch) {
        if (allowedSourceOffset(offsetAndEpoch, state.localMaterializedOffsetAndEpoch)
                && allowedStateOffset(metadata.stateOffsetAndEpoch(), state.restoreOffsetAndEpoch)) {
            try {
                if (state.status() != TierPartitionStatus.ERROR)
                    throw new IllegalStateException(String.format("TierPartitionState %s "
                                    + "was expected to be in ERROR state when restoring state via "
                                    + "metadata %s with target status %s at offsetEpoch %s", state,
                            metadata, targetStatus, offsetAndEpoch));

                log.debug("Restoring TierPartitionState for {} from object storage due to "
                        + "event {}", topicIdPartition, metadata);
                final Path restorePath = recoverPath(basePath);
                FileChannel channel = FileChannel.open(restorePath,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.READ, StandardOpenOption.WRITE);
                channel.write(targetState);
                channel.force(true);

                Optional<Header> initialHeaderOpt = readHeader(channel);
                if (!initialHeaderOpt.isPresent())
                    throw new IllegalStateException(String.format("TierPartitionState being restored "
                            + "does not contain a valid header, aborting restore. Metadata %s "
                                    + "with target status %s at offsetEpoch %s",
                            metadata, targetState, offsetAndEpoch));

                channel = maybeMigrateHeader(topicPartition, basePath, version, restorePath,
                        channel, initialHeaderOpt.get());

                // The new state will initially be opened at restorePath and then swapped in two phases,
                // first moving the mutable file path to the discarded file path and then moving the restore
                // path to the mutable file path. This is safe because the FileChannel still uses a valid
                // FD for the correct files even though their location has moved.
                final State newState = State.createRestoredState(topicPartition, basePath, version,
                        channel, ioExceptionHandler, offsetAndEpoch, OffsetAndEpoch.EMPTY, targetStatus);

                final State oldState = state;
                safeSwapForRestoredState(offsetAndEpoch, restorePath, newState);
                log.info("Restored TierPartitionState for {} from object storage due to "
                                + "event {}, old state: {} new state: {}", topicIdPartition,
                        metadata, oldState, newState);
                oldState.close();
                return RestoreResult.SUCCEEDED;
            } catch (IOException ioe) {
                TierPartitionStatus previousStatus = state.status();
                // Handle IOException specially by marking the dir offline, as it indicates a
                // serious enough error that we can't ignore. This may halt the broker eventually.
                state.setErrorStatus(offsetAndEpoch, false);
                ioExceptionHandler.accept(ioe);
                throw new KafkaStorageException(
                        "Failed to restore state " + metadata + ", currentEpoch=" + state.currentEpoch +
                                ", tierTopicPartitionOffsetAndEpoch=" + offsetAndEpoch +
                                ", previousTierPartitionStatus=" + previousStatus +
                                ", newTierPartitionStatus=" + TierPartitionStatus.ERROR, ioe);
            } catch (Exception e) {
                // re-fence state if recovery fails
                TierPartitionStatus previousStatus = state.status();
                state.setErrorStatus(offsetAndEpoch, false);
                String logMsg = String.format(
                        "Failed to restore state %s, currentEpoch=%d, "
                                + "tierTopicPartitionOffsetAndEpoch=%s" +
                                ", previousTierPartitionStatus=%s, newTierPartitionStatus=%s",
                        metadata, state.currentEpoch, offsetAndEpoch, previousStatus,
                        TierPartitionStatus.ERROR);
                log.error(logMsg, e);
                return RestoreResult.FAILED;
            }
        } else {
            log.info("Ignoring state recovery {} at offset {} as last materialized offset is {} for {}",
                    metadata, offsetAndEpoch, state.localMaterializedOffsetAndEpoch, topicIdPartition);
            return RestoreResult.FAILED;
        }
    }

    private void safeSwapForRestoredState(OffsetAndEpoch offsetAndEpoch, Path restorePath, State newState) throws IOException {
        boolean movedMutableFile = false;
        boolean movedRestoreFile = false;
        try {
            Utils.atomicMoveWithFallback(mutableFilePath(basePath), discardedFilePath(basePath, offsetAndEpoch));
            movedMutableFile = true;
            Utils.atomicMoveWithFallback(restorePath, mutableFilePath(basePath));
            movedRestoreFile = true;
            newState.flush();
        } catch (Exception e) {
            if (movedRestoreFile)
                Utils.atomicMoveWithFallback(mutableFilePath(basePath), restorePath);
            if (movedMutableFile)
                Utils.atomicMoveWithFallback(discardedFilePath(basePath, offsetAndEpoch), mutableFilePath(basePath));
            newState.close();
            throw e;
        }

        state = newState;
    }

    @SuppressWarnings("deprecation")
    private void maybeOpenChannel() throws IOException {
        stateResourceReadWriteLock.writeLock().lock();
        try {
            synchronized (stateLock) {
                if (tieringEnabled && !state.status.isOpen()) {
                    Path flushedFilePath = flushedFilePath(basePath);
                    Path mutableFilePath = mutableFilePath(basePath);

                    if (!Files.exists(flushedFilePath))
                        Files.createFile(flushedFilePath);
                    Files.copy(flushedFilePath, mutableFilePath, StandardCopyOption.REPLACE_EXISTING);

                    FileChannel channel = getChannelMaybeReinitialize(topicPartition, topicIdPartition, basePath, version);
                    if (channel == null) {
                        state = State.EMPTY;
                        return;
                    }

                    try {
                        state = new State(topicPartition, basePath, version, channel, ioExceptionHandler);
                        topicIdPartition = state.topicIdPartition;
                    } catch (Exception e) {
                        // Found exception while initializing the TierMetadataStates from flushed file. Till we
                        // have solution for gracefully recovery from exception/corruption from state file, we will crash the
                        // broker and expect manual recovery. Not doing so can potentially lead to ignoring
                        // all or part of the tier data during loading of log and causing data loss. By crashing the broker we
                        // are forcing leader election to another replica.
                        try {
                            backupState(topicIdPartition, basePath, errorFilePath(basePath));
                            closeHandlersImpl();
                        } catch (Exception exceptionToIgnore) {
                            log.warn("Failed to backup / close tier partition state for {}", topicIdPartition, exceptionToIgnore);
                        }

                        // Send IOException to logDirFailureChannel, which will cause the disk to go offline.
                        IOException ioexp = new IOException("Exception in initializing TierMetadataState for " + topicIdPartition, e);
                        ioExceptionHandler.accept(ioexp);
                        throw new KafkaStorageException(ioexp);
                    }
                }
            }
        } finally {
            stateResourceReadWriteLock.writeLock().unlock();
        }
    }

    /**
     * closeHandlers implementation without locking for internal FileTierPartitionState use
     * Both the stateHandleReadWriteLock and the stateLock should be taken by the caller
     * @throws IOException if State#close throws an IOException
     */
    private void closeHandlersImpl() throws IOException {
        if (state.status != TierPartitionStatus.UNINITIALIZED) {
            try {
                state.close();
            } finally {
                state = State.EMPTY;
            }
        }
    }

    private static void writeHeader(FileChannel channel, Header header) throws IOException {
        final int remaining = header.payloadBuffer().remaining();
        final short sizePrefix = (short) remaining;
        if (sizePrefix != remaining)
            throw new IllegalStateException(String.format("Unexpected header size: %d", remaining));
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

    private static CloseableIterator<TierObjectMetadata> tierObjectMetadataCloseableIterator(
            Iterator<TierObjectMetadata> iterator, ReentrantReadWriteLock stateHandleReadWriteLock) {
        return new CloseableIterator<TierObjectMetadata>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public TierObjectMetadata next() {
                return iterator.next();
            }

            @Override
            public void close() {
                stateHandleReadWriteLock.readLock().unlock();
            }
        };
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
                            version,
                            -1,
                            TierPartitionStatus.INIT,
                            -1L,
                            -1L,
                            OffsetAndEpoch.EMPTY,
                            OffsetAndEpoch.EMPTY,
                            OffsetAndEpoch.EMPTY,
                            OffsetAndEpoch.EMPTY));
                } else {
                    channel.close();
                    return null;
                }
            } else {
                channel = maybeMigrateHeader(topicPartition, basePath, version, mutableFilePath, channel, initialHeaderOpt.get());
            }
        } catch (IOException e) {
            channel.close();
            throw e;
        }
        return channel;
    }

    /**
     * Migrates a file state referenced by a file channel to a version with the latest header
     * @param topicPartition topic partition for this channel
     * @param basePath the base path where this partition is stored
     * @param requiredVersion the state version number required. States with a different version
     *                        number will be migrated.
     * @param destination the destination path to migrate to
     * @param channel an open FileChannel to migrate from
     * @param existingHeader the header for the state at the file channel being passed
     * @return an open FileChannel with the newly migrated state
     * @throws IOException
     */
    private static FileChannel maybeMigrateHeader(TopicPartition topicPartition, String basePath,
                                                  byte requiredVersion, Path destination,
                                                  FileChannel channel,
                                                  Header existingHeader) throws IOException {
        if (existingHeader.version() != requiredVersion) {
            TopicIdPartition topicIdPartition;
            Path tmpFilePath = tmpFilePath(basePath);
            topicIdPartition = new TopicIdPartition(topicPartition.topic(),
                    existingHeader.topicId(),
                    topicPartition.partition());

            try (FileChannel tmpChannel = FileChannel.open(tmpFilePath, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.READ,
                    StandardOpenOption.WRITE)) {
                log.info("Rewriting tier partition state with version {} to {} for {}",
                        existingHeader.version(), requiredVersion, topicIdPartition);
                Header newHeader = new Header(topicIdPartition.topicId(),
                        requiredVersion,
                        existingHeader.tierEpoch(),
                        existingHeader.status(),
                        existingHeader.startOffset(),
                        existingHeader.endOffset(),
                        existingHeader.globalMaterializedOffsetAndEpoch(),
                        existingHeader.localMaterializedOffsetAndEpoch(),
                        existingHeader.errorOffsetAndEpoch(),
                        existingHeader.restoreOffsetAndEpoch());
                writeHeader(tmpChannel, newHeader);
                tmpChannel.position(newHeader.size());
                channel.position(existingHeader.size());
                copy(channel, tmpChannel);
            }
            Utils.atomicMoveWithFallback(tmpFilePath, destination);
            channel.close();
            return FileChannel.open(destination, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } else {
            return channel;
        }
    }

    // visible for testing
    boolean dirty() {
        stateResourceReadWriteLock.readLock().lock();
        try {
            synchronized (stateLock) {
                return state.dirty;
            }
        } finally {
            stateResourceReadWriteLock.readLock().unlock();
        }
    }

    /**
     * When a message has an offset > last processed offset, it must have an epoch >= last
     * processed epoch. If it does not, this indicates log divergence or another similar lineage
     * problem. Validate that the epoch of the message being processed is greater than or equal to
     * the current epoch, allowing messages if the current epoch is not known
     * @param current the current epoch
     * @param checked the epoch for the message being processed
     * @throws IllegalStateException if a lineage issue is detected
     */
    private static void validateEpoch(Optional<Integer> current, Optional<Integer> checked) {
        if (current.isPresent() && checked.isPresent() && checked.get() < current.get())
            throw new IllegalStateException("New epoch " + checked + " must dominate old "
                    + "epoch " + current);
    }

    /**
     * Returns true if the state offset of a AbstractTierMetadata is greater than or equal to
     * the latest TierPartitionState restore, or if the state offset is EMPTY, indicating
     * an older format message or a message type that does not require this validation.
     * @param stateOffsetAndEpoch metadata state offset and epoch
     * @param current the restore offset and epoch for the state
     * @return true if the message with stateOffsetAndEpoch should be applied
     */
    private static boolean allowedStateOffset(OffsetAndEpoch stateOffsetAndEpoch,
                                              OffsetAndEpoch current) {
        if (stateOffsetAndEpoch.equals(OffsetAndEpoch.EMPTY))
            return true;

        if (stateOffsetAndEpoch.offset() >= current.offset()) {
            validateEpoch(current.epoch(), stateOffsetAndEpoch.epoch());
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check whether a metadata's source offset and epoch is greater than the current
     * materialization point for a state.
     * @param toProcess the OffsetAndEpoch for the metadata being processed
     * @param current the current OffsetAndEpoch point materialized for the state
     * @return true if the message with offset toProcess should be applied
     */
    private static boolean allowedSourceOffset(OffsetAndEpoch toProcess, OffsetAndEpoch current) {
        if (toProcess.offset() > current.offset()) {
            validateEpoch(current.epoch(), toProcess.epoch());
            return true;
        } else {
            // This is a duplicate message with an offset that has already been materialized into
            // the tier state.
            // This can happen for a couple reasons:
            // 1. We may have transitioned from CATCHUP status to ONLINE status and metadata
            //    may have been replayed using the new consumer.
            // 2. During recovery, the TierTopicConsumer may re-consume offsets that have already
            //    been materialized as the tier.offsets may not be consistent with the flushed
            //    TierPartitionState.
            //
            // We perform one final check to ensure that the epoch isn't greater than the current
            // epoch indicating divergence
            if (toProcess.epoch().isPresent() && current.epoch().isPresent()
                    && toProcess.epoch().get() > current.epoch().get()) {
                throw new IllegalStateException("Incorrect epoch in " + toProcess + " with current epoch " + current);
            } else {
                return false;
            }
        }
    }

    // visible for testing
    static Path flushedFilePath(String basePath) {
        return StateFileType.FLUSHED.filePath(basePath);
    }

    // visible for testing
    static Path mutableFilePath(String basePath) {
        return StateFileType.MUTABLE.filePath(basePath);
    }

    // visible for testing
    static Path recoverPath(String basePath) {
        return StateFileType.RECOVER.filePath(basePath);
    }

    // visible for testing
    static Path discardedFilePath(String basePath, OffsetAndEpoch restoreAt) {
        return Paths.get(String.format("%s.%s_epoch_%s_offset_%s",
                basePath,
                StateFileType.DISCARDED,
                restoreAt.epoch().orElse(-1),
                restoreAt.offset()));
    }

    // visible for testing
    static Path tmpFilePath(String basePath) {
        return StateFileType.TEMPORARY.filePath(basePath);
    }

    // visible for testing
    static Path errorFilePath(String basePath) {
        return StateFileType.ERROR.filePath(basePath);
    }

    private static class State {
        private final static State EMPTY = new State();

        // the mutable file channel
        private final FileChannel channel;
        // valid (unfenced) segment UUIDs
        private final ConcurrentNavigableMap<Long, UUID> validSegments = new ConcurrentSkipListMap<>();
        // All segments and their states, referenced by UUID
        private final ConcurrentNavigableMap<UUID, SegmentState> allSegments = new ConcurrentSkipListMap<>();
        // state format version
        private final byte version;
        private final Consumer<IOException> ioExceptionHandler;

        private String basePath;
        private TopicIdPartition topicIdPartition = null;
        private TierObjectMetadata uploadInProgress;  // cached for quick lookup
        // boolean denoting whether the state has been mutated and is unflushed
        private boolean dirty = false;
        // used to track materialization up to a desired offset
        private volatile ReplicationMaterializationListener materializationListener;
        // used to track materialization up to a desired leader epoch
        private volatile LeaderEpochMaterializationListener leaderEpochMaterializationTracker;
        // materialized end offset
        private volatile long endOffset = -1L;
        // end offset of the flush state file
        private volatile long committedEndOffset = -1L;
        // epoch of the latest materialized InitLeader message
        private volatile int currentEpoch = -1;
        // total size of unfenced segments
        private volatile long validSegmentsSize = 0;
        // overall status of this partition state
        private volatile TierPartitionStatus status = TierPartitionStatus.UNINITIALIZED;
        // offset and epoch for the _confluent-tier-state topic partition of the latest consumed message
        private volatile OffsetAndEpoch globalMaterializedOffsetAndEpoch = OffsetAndEpoch.EMPTY;
        // offset and epoch for the tier state topic partition of the latest materialized message
        private volatile OffsetAndEpoch localMaterializedOffsetAndEpoch = OffsetAndEpoch.EMPTY;
        // offset and epoch for the tier state topic partition of the earliest known message
        // that caused an error and triggered fencing (this includes a PartitionFence event)
        private volatile OffsetAndEpoch errorOffsetAndEpoch = OffsetAndEpoch.EMPTY;
        // offset and epoch denoting the offset where TierForceRestore occurred, if any
        private volatile OffsetAndEpoch restoreOffsetAndEpoch = OffsetAndEpoch.EMPTY;
        // true if error status was reached via PartitionFence event, false otherwise
        private volatile boolean errorStatusReachedViaFenceEvent = false;

        private State() {
            channel = null;
            version = -1;
            basePath = null;
            ioExceptionHandler = e -> {
                throw new IllegalStateException("Illegal use of setLogDirOffline");
            };
        }

        State(TopicPartition topicPartition,
              String basePath,
              byte version,
              FileChannel channel,
              Consumer<IOException> ioExceptionHandler) throws IOException, StateCorruptedException {
            this.basePath = basePath;
            this.version = version;
            this.channel = channel;
            this.ioExceptionHandler = ioExceptionHandler;
            scanAndInitialize(topicPartition);
            if (status == TierPartitionStatus.UNINITIALIZED)
                throw new IllegalStateException("Illegal TierPartitionStatus: " + status);
        }

        private static State createRestoredState(TopicPartition topicPartition,
                                                 String basePath,
                                                 byte version,
                                                 FileChannel channel,
                                                 Consumer<IOException> ioExceptionHandler,
                                                 OffsetAndEpoch localMaterializedOffsetAndEpoch,
                                                 OffsetAndEpoch globalMaterializedOffsetAndEpoch,
                                                 TierPartitionStatus status) throws IOException {
            State imported = new State(topicPartition, basePath, version, channel,
                    ioExceptionHandler);
            imported.localMaterializedOffsetAndEpoch = localMaterializedOffsetAndEpoch;
            imported.globalMaterializedOffsetAndEpoch = globalMaterializedOffsetAndEpoch;
            imported.restoreOffsetAndEpoch = localMaterializedOffsetAndEpoch;
            imported.setStatus(status);
            imported.dirty = true;
            return imported;
        }

        private void scanAndInitialize(TopicPartition topicPartition) throws IOException, StateCorruptedException {
            log.debug("scan and truncate TierPartitionState {}", topicPartition);
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

            if (header.endOffset() != -1 && endOffset != header.endOffset()) {
                if (numSegments() > 0)
                    log.info("File header endOffset does not match the materialized endOffset. Setting state endOffset to be " +
                            "equal to header endOffset. Header endOffset: " + header.endOffset() + " materialized state endOffset: " +
                            endOffset + " for partition " + topicIdPartition);
                endOffset = header.endOffset();
            }

            channel.position(channel.size());
            committedEndOffset = endOffset;
            currentEpoch = header.tierEpoch();
            globalMaterializedOffsetAndEpoch = header.globalMaterializedOffsetAndEpoch();
            localMaterializedOffsetAndEpoch = header.localMaterializedOffsetAndEpoch();
            errorOffsetAndEpoch = header.errorOffsetAndEpoch();
            status = header.status();

            log.info("Opened tier partition state for {} in status {}. topicIdPartition: {} tierEpoch: {} endOffset: {}",
                    topicPartition, status, topicIdPartition, currentEpoch, endOffset);
        }

        public void updateBasePath(String path) {
            basePath = path;
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

        private TierPartitionStatus getStatus() {
            return status;
        }

        private void setStatus(TierPartitionStatus status) {
            // TierPartitionStatus.UNINITIALIZED is special, as it is only used to indicate a dummy empty state.
            // Therefore, once the object has reached this status, we explicitly check that it
            // can not be changed again, nor can a state be transitioned to UNINITIALIZED
            if (this.status == TierPartitionStatus.UNINITIALIZED || status == TierPartitionStatus.UNINITIALIZED)
                throw new IllegalStateException("Illegal transition " + this.status + " to " + status);

            if (this.status != status) {
                this.status = status;
                dirty = true;
                log.info("Status updated to {} for {}", status, topicIdPartition);
            }
        }

        private void setErrorStatus(
            OffsetAndEpoch offsetAndEpoch, boolean errorStatusReachedViaFenceEvent) {
            errorOffsetAndEpoch = offsetAndEpoch;
            this.errorStatusReachedViaFenceEvent = errorStatusReachedViaFenceEvent;
            setStatus(TierPartitionStatus.ERROR);
        }

        public void beginCatchup() {
            if (!status.isOpenForWrite())
                throw new IllegalStateException("Illegal state " + status + " for tier partition basePath: " + basePath);

            setStatus(TierPartitionStatus.CATCHUP);
        }

        public void onCatchUpComplete() {
            if (!status.isOpenForWrite())
                throw new IllegalStateException("Illegal state " + status + " for tier partition basePath: " + basePath);

            setStatus(TierPartitionStatus.ONLINE);
        }

        public Optional<Long> startOffset() {
            // be careful to generate startOffset consistently as it is accessed without a lock
            Map.Entry<Long, UUID> firstEntry = validSegments.firstEntry();
            if (firstEntry != null)
                return Optional.of(firstEntry.getKey());
            return Optional.empty();
        }

        public Long endOffset() {
            // accessed without a lock
            return endOffset;
        }

        TierPartitionStatus status() {
            // accessed without a lock
            return status;
        }

        int currentEpoch() {
            // accessed without a lock
            return currentEpoch;
        }

        public int numSegments() {
            // accessed without a lock
            return validSegments.size();
        }

        long committedEndOffset() {
            // accessed without a lock
            return committedEndOffset;
        }

        long totalSize() {
            // accessed without a lock
            return validSegmentsSize;
        }

        private NavigableSet<Long> segmentOffsets() {
            return validSegments.keySet();
        }

        public NavigableSet<Long> segmentOffsets(long from, long to) {
            return Log$.MODULE$.logSegments(validSegments, from, to).keySet();
        }

        public Iterator<TierObjectMetadata> segments() {
            return new TierObjectMetadataIterator(segmentOffsets().iterator());
        }

        public Iterator<TierObjectMetadata> segments(long from, long to) {
            return new TierObjectMetadataIterator(segmentOffsets(from, to).iterator());
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

        /**
         * Reads the first segment object metadata in SEGMENT_UPLOAD_COMPLETE state
         * with offset >= targetOffset. This is intended to be used when a consumer / reader wishes
         * to read records with offset >= targetOffset as it will skip over any segments with
         * endOffset < targetOffset.
         *
         * @param topicIdPartition    TopicIdPartition for tier partition state being read
         * @param initialBytePosition the initial byte position for the FileTierPartitionIterator
         * @param targetOffset        the target offset to be read
         * @return optional TierObjectMetadata for a containing data with offsets >= targetOffset
         * @throws IOException
         */
        private Optional<TierObjectMetadata> readValidObjectMetadata(TopicIdPartition topicIdPartition,
                                                                     long initialBytePosition,
                                                                     long targetOffset) throws IOException {
            if (!validSegments.isEmpty()) {
                FileTierPartitionIterator iterator = iterator(topicIdPartition, channel, initialBytePosition);
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

        // Caller must hold FileTierPartitionState.lock
        private AppendResult appendMetadata(AbstractTierMetadata entry,
                                            OffsetAndEpoch offsetAndEpoch) throws KafkaStorageException {
            if (status.hasError()) {
                log.debug("Skipping processing for {} from offset {} as the current status is "
                        + "failed", entry, offsetAndEpoch);
                return AppendResult.FAILED;
            }

            if (!status.isOpenForWrite()) {
                log.debug("Skipping processing for {} from offset {} as file is not open for write",
                        entry, offsetAndEpoch);
                return AppendResult.NOT_TIERABLE;
            }

            try {
                if (!allowedSourceOffset(offsetAndEpoch, localMaterializedOffsetAndEpoch)) {
                    log.debug("Ignoring message at offset {} as last materialized offset is {} for {}",
                            offsetAndEpoch, localMaterializedOffsetAndEpoch, topicIdPartition);
                    return AppendResult.FENCED;
                }

                if (!allowedStateOffset(entry.stateOffsetAndEpoch(), restoreOffsetAndEpoch)) {
                    log.debug("Ignoring message {} at offset {} as the restore offset is {} for "
                                    + "{}", entry,  offsetAndEpoch, entry.stateOffsetAndEpoch(),
                            topicIdPartition);
                    return AppendResult.RESTORE_FENCED;
                }

                AppendResult result = appendMetadataImpl(entry, offsetAndEpoch);
                localMaterializedOffsetAndEpoch = offsetAndEpoch;
                log.debug("Processed append for {} with result {} consumed from offset {}", entry,
                        result, offsetAndEpoch);
                return result;
            } catch (IOException ioe) {
                TierPartitionStatus previousStatus = getStatus();
                // Handle IOException specially by marking the dir offline, as it indicates a
                // serious enough error that we can't ignore. This may halt the broker eventually.
                setErrorStatus(offsetAndEpoch, false);
                ioExceptionHandler.accept(ioe);
                throw new KafkaStorageException(
                        "Failed to apply " + entry + ", currentEpoch=" + currentEpoch +
                                ", tierTopicPartitionOffsetAndEpoch=" + offsetAndEpoch +
                                ", previousTierPartitionStatus=" + previousStatus +
                                ", newTierPartitionStatus=" + TierPartitionStatus.ERROR, ioe);
            } catch (Exception e) {
                TierPartitionStatus previousStatus = getStatus();
                setErrorStatus(offsetAndEpoch, false);
                String logMsg = String.format(
                        "Failed to apply %s, currentEpoch=%d, tierTopicPartitionOffsetAndEpoch=%s" +
                                ", previousTierPartitionStatus=%s, newTierPartitionStatus=%s",
                        entry, currentEpoch, offsetAndEpoch, previousStatus,
                        TierPartitionStatus.ERROR);
                if (previousStatus == TierPartitionStatus.ONLINE) {
                    // To avoid noisy logging, we only log with error level, if a partition reaches
                    // TierPartitionStatus.ERROR status while previously being in
                    // TierPartitionStatus.ONLINE status.
                    log.error(logMsg, e);
                } else {
                    log.info(logMsg, e);
                }

                return AppendResult.FAILED;
            }
        }

        private AppendResult appendMetadataImpl(
            AbstractTierMetadata entry, OffsetAndEpoch offsetAndEpoch) throws IOException {
            switch (entry.type()) {
                case InitLeader:
                    return handleInitLeader((TierTopicInitLeader) entry);
                case PartitionFence:
                    return handlePartitionFence((TierPartitionFence) entry, offsetAndEpoch);

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

        private List<TierObjectMetadata> metadataForStates(Set<TierObjectMetadata.State> states) {
            return allSegments.values()
                    .stream()
                    .filter(segmentState -> states.contains(segmentState.state))
                    .map(segmentState -> {
                        try {
                            return iterator(topicIdPartition, channel, segmentState.position).next();
                        } catch (IOException e) {
                            throw new KafkaStorageException(e);
                        }
                    })
                    .collect(Collectors.toList());
        }

        public String toString() {
            // all read accesses below are safe without a lock
            return "State(startOffset=" + startOffset()
                    + ", endOffset=" + endOffset()
                    + ", committedEndOffset=" + committedEndOffset()
                    + ", numSegments=" + numSegments()
                    + ", tierEpoch=" + currentEpoch
                    + ", lastMaterializedOffset=" + localMaterializedOffsetAndEpoch
                    + ", restoreOffsetAndEpoch=" + restoreOffsetAndEpoch
                    + ")";
        }

        public Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException {
            Map.Entry<Long, UUID> entry = validSegments.floorEntry(targetOffset);
            if (entry != null)
                return readValidObjectMetadata(topicIdPartition, position(entry.getValue()), targetOffset);
            else
                return Optional.empty();
        }

        private AppendResult handlePartitionFence(
            TierPartitionFence partitionFence, OffsetAndEpoch offsetAndEpoch) {
            setErrorStatus(offsetAndEpoch, true);
            log.info(
                "topicIdPartition={} fenced by PartitionFence event={} at offset={}",
                topicIdPartition, partitionFence, offsetAndEpoch);
            return AppendResult.FAILED;
        }

        private AppendResult handleInitLeader(TierTopicInitLeader initLeader) throws IOException {
            // We accept any epoch >= the current one, as there could be duplicate init leader messages for the current
            // epoch.
            if (initLeader.tierEpoch() == currentEpoch) {
                return AppendResult.ACCEPTED;
            } else if (initLeader.tierEpoch() > currentEpoch) {
                // On leader change, we fence all in-progress uploads and segments that were being deleted
                Set<TierObjectMetadata.State> statesToFence = new HashSet<>(Arrays.asList(
                        TierObjectMetadata.State.SEGMENT_UPLOAD_INITIATE,
                        TierObjectMetadata.State.SEGMENT_DELETE_INITIATE)
                );
                List<TierObjectMetadata> toFence = metadataForStates(statesToFence);
                for (TierObjectMetadata metadata : toFence)
                    fenceSegment(metadata);

                currentEpoch = initLeader.tierEpoch();
                dirty = true;
                if (leaderEpochMaterializationTracker != null)
                    maybeCompleteLeaderEpochMaterializationTracker(currentEpoch);
                return AppendResult.ACCEPTED;
            } else {
                return AppendResult.FENCED;
            }
        }

        private AppendResult maybeTransitionSegment(AbstractTierSegmentMetadata metadata) throws IOException {
            // We disallow transitions that belong to an epoch greater than the current epoch.
            // This scenario indicates that we missed at least one InitLeader message, and should
            // therefore be considered a serious error.
            if (metadata.tierEpoch() > currentEpoch) {
                throw new IllegalStateException(
                        String.format(
                                "Unexpected transition attempted for topicIdPartition=%s via metadata=%s" +
                                        " at epoch=%s while currentEpoch=%s is lower",
                                topicIdPartition,
                                metadata,
                                metadata.tierEpoch(),
                                currentEpoch));
            }
            // We fence transitions that belong to an epoch lower than the current epoch, because
            // the event is generated by the prior leader. Note that we do not need to track
            // SegmentUploadInitiate messages that are fenced due to epoch changes for later deletion.
            // The prior leader will also see a fenced transition, and will not stage an upload.
            if (metadata.tierEpoch() < currentEpoch) {
                log.info(
                        "Fenced {} as currentEpoch={} ({})", metadata, currentEpoch, topicIdPartition);
                return AppendResult.FENCED;
            }


            SegmentState currentState = getState(metadata.objectId());
            if (currentState != null) {
                if (currentState.state.equals(metadata.state())) {
                    // This is a duplicate transition
                    log.debug("Accepting duplicate transition for {} ({})", metadata, topicIdPartition);
                    return AppendResult.ACCEPTED;
                } else if (!currentState.state.canTransitionTo(metadata.state())) {
                    // We have seen this transition before so fence it. This can only happen in couple of scenarios:
                    // Given that we have a stronger bound on the caller based on the last materialized offset,
                    // we won't run into the reprocessing use-case. However, we can still run into the following scenario:
                    // 1. The producer retried a message that was successfully written but was not
                    //    acked. Retries will be fenced knowing that:
                    //       a) any future completed by the TierTopicManager will have been completed correctly by the
                    //          previous copy of this message.
                    //       b) This fencing will not be problematic to the archiver due to 1(a)
                    //          completing the materialization correctly.
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

        private void addSegmentMetadata(TierObjectMetadata metadata, long byteOffset) {
            SegmentState segmentState = updateAndGetState(byteOffset, metadata);
            switch (metadata.state()) {
                case SEGMENT_UPLOAD_INITIATE:
                    if (uploadInProgress != null)
                        throw new IllegalStateException("Unexpected upload in progress " + uploadInProgress +
                                " when appending " + metadata + " to " + topicIdPartition);
                    uploadInProgress = metadata.duplicate();
                    break;

                case SEGMENT_UPLOAD_COMPLETE:
                    putValid(segmentState, metadata);
                    uploadInProgress = null;
                    break;

                case SEGMENT_DELETE_INITIATE:
                    removeValid(segmentState, metadata);
                    break;

                case SEGMENT_DELETE_COMPLETE:
                case SEGMENT_FENCED:
                    break;

                default:
                    throw new IllegalArgumentException("Unknown state " + metadata + " for " + topicIdPartition);
            }
        }

        private TierObjectMetadata updateState(UUID objectId, TierObjectMetadata.State newState) throws IOException {
            SegmentState currentState = getState(objectId);
            if (currentState == null)
                throw new IllegalStateException("No metadata found for " + objectId + " in " + topicIdPartition);

            TierObjectMetadata metadata = iterator(topicIdPartition, channel, currentState.position).next();

            if (!objectId.equals(metadata.objectId()))
                throw new IllegalStateException("id mismatch. Expected: " + objectId + " Got: " + metadata.objectId() + " Partition: " + topicIdPartition);

            int oldSize = metadata.payloadSize();
            metadata.mutateState(newState);
            int newSize = metadata.payloadSize();
            if (oldSize != newSize) {
                throw new IllegalStateException(
                        String.format("Size mismatch for objectId %s, expected: %d, got: %d, topicIdPartition: %s.",
                                metadata.objectId(), oldSize, newSize, topicIdPartition));
            }
            Utils.writeFully(channel, currentState.position + ENTRY_LENGTH_SIZE, metadata.payloadBuffer());
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

            if (metadata.endOffset() > endOffset) {
                // This is the next in line valid segment to upload belonging to this epoch. Fence any in-progress upload.
                if (uploadInProgress != null)
                    fenceSegment(uploadInProgress);

                ByteBuffer metadataBuffer = metadata.payloadBuffer();
                long byteOffset = appendWithSizePrefix(metadataBuffer);
                addSegmentMetadata(metadata, byteOffset);
                dirty = true;
                return AppendResult.ACCEPTED;
            }

            // This attempt to upload a segment must be fenced as it covers an offset range that has already been uploaded successfully.
            log.info("Fencing uploadInitiate for {}. currentEndOffset={} currentEpoch={}. ({})",
                    metadata, endOffset, currentEpoch, topicIdPartition);
            return AppendResult.FENCED;
        }

        private AppendResult handleUploadComplete(TierSegmentUploadComplete uploadComplete) throws IOException {
            if (!uploadInProgress.objectId().equals(uploadComplete.objectId()))
                throw new IllegalStateException("Expected " + uploadInProgress.objectId() + " to be in-progress " +
                        "but got " + uploadComplete.objectId() + " for partition " + topicIdPartition);

            TierObjectMetadata metadata = updateState(uploadComplete.objectId(), TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE);

            // Try completing materialization tracker, now that we have materialized a new segment
            if (materializationListener != null)
                maybeCompleteMaterializationTracker(metadata);

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

        private void maybeCompleteLeaderEpochMaterializationTracker(int tierEpoch) throws IOException {
            if (leaderEpochMaterializationTracker.canComplete(tierEpoch)) {
                TierObjectMetadata lastObjectMetadata = null;
                if (!validSegments.keySet().isEmpty()) {
                    Optional<TierObjectMetadata> lastObjectMetadataOpt = metadata(validSegments.keySet().last());
                    if (lastObjectMetadataOpt.isPresent())
                        lastObjectMetadata = lastObjectMetadataOpt.get();
                }
                flush();
                leaderEpochMaterializationTracker.complete(lastObjectMetadata);
                leaderEpochMaterializationTracker = null;
            }
        }

        private void completeLeaderEpochMaterializationTrackerExceptionally(Exception e) {
            leaderEpochMaterializationTracker.completeExceptionally(e);
            leaderEpochMaterializationTracker = null;
        }

        public CompletableFuture<Optional<TierObjectMetadata>> materializeUpto(int targetEpoch) throws IOException {
                if (leaderEpochMaterializationTracker != null)
                    completeLeaderEpochMaterializationTrackerExceptionally(
                            new IllegalStateException("Duplicate leader epoch materialization listener registration for " + topicIdPartition));

                leaderEpochMaterializationTracker = new LeaderEpochMaterializationListener(log, topicIdPartition, targetEpoch);
                CompletableFuture<Optional<TierObjectMetadata>> promise = leaderEpochMaterializationTracker.promise();

                if (status.isOpen()) {
                    maybeCompleteLeaderEpochMaterializationTracker(currentEpoch);
                } else {
                    completeLeaderEpochMaterializationTrackerExceptionally(new TierPartitionStateIllegalListenerException(
                            "Tier partition state for " + topicIdPartition + " is not open."));
                }
                return promise;
        }

        private void maybeCompleteMaterializationTracker(TierObjectMetadata lastMaterializedSegment) throws IOException {
            if (materializationListener.canComplete(lastMaterializedSegment)) {
                // flush to ensure readable TierPartitionState aligns with the local log
                // that will be fetched by the Replica Fetcher. Otherwise we could end up in an unsafe
                // state where the TierPartitionState doesn't align if the broker shuts down
                flush();
                materializationListener.complete(lastMaterializedSegment);
                materializationListener = null;
            }
        }

        private void completeMaterializationTrackerExceptionally(Exception e) {
            materializationListener.completeExceptionally(e);
            materializationListener = null;
        }

        public Future<TierObjectMetadata> materializationListener(long targetOffset) throws IOException {
            if (materializationListener != null)
                completeMaterializationTrackerExceptionally(
                        new IllegalStateException("Duplicate materialization listener registration for " + topicIdPartition));

            materializationListener = new ReplicationMaterializationListener(log, topicIdPartition, targetOffset);
            log.info("Registered materialization listener {}. targetOffset: {}, currentEndOffset: {}, currentCommittedEndOffset: {}.",
                    materializationListener, targetOffset, endOffset, committedEndOffset);

            Future<TierObjectMetadata> promise = materializationListener.promise();

            if (status.isOpen()) {
                Optional<TierObjectMetadata> metadata = Optional.empty();
                long uncommittedEndOffset = endOffset;
                if (uncommittedEndOffset != -1L && targetOffset <= uncommittedEndOffset)
                    metadata = metadata(targetOffset);

                if (metadata.isPresent()) {
                    if (metadata.get().endOffset() < targetOffset)
                        throw new IllegalStateException("Metadata lookup for offset " + targetOffset +
                                " returned unexpected segment " + metadata + " for " + topicIdPartition);
                    maybeCompleteMaterializationTracker(metadata.get());
                }
            } else {
                completeMaterializationTrackerExceptionally(new TierPartitionStateIllegalListenerException(
                        "Tier partition state for " + topicIdPartition + " is not open."));
            }
            return promise;
        }

        private void close() throws IOException {
            if (materializationListener != null) {
                completeMaterializationTrackerExceptionally(
                        new TierPartitionStateIllegalListenerException("Tier partition state for " +
                                topicIdPartition + " has been closed."));
            }
            if (leaderEpochMaterializationTracker != null) {
                completeLeaderEpochMaterializationTrackerExceptionally(
                        new TierPartitionStateIllegalListenerException("Tier partition state for " +
                                topicIdPartition + " has been closed."));
            }

            if (channel != null)
                channel.close();
        }

        private void flush() throws IOException {
            if (!dirty)
                return;
            if (status.hasError()) {
                flushErrorState();
                dirty = false;
            } else if (status.isOpenForWrite()) {
                flushWritableState();
                dirty = false;
            }
        }

        private void flushWritableState() throws IOException {
            // update the header and flush file contents
            writeHeader(channel, new Header(
                    topicIdPartition.topicId(),
                    version,
                    currentEpoch,
                    status,
                    startOffset().orElse(-1L),
                    endOffset,
                    globalMaterializedOffsetAndEpoch,
                    localMaterializedOffsetAndEpoch,
                    errorOffsetAndEpoch,
                    restoreOffsetAndEpoch));
            channel.force(true);

            // move file contents to the flushed file
            Files.copy(mutableFilePath(basePath), tmpFilePath(basePath),
                    StandardCopyOption.REPLACE_EXISTING);
            Utils.atomicMoveWithFallback(tmpFilePath(basePath), flushedFilePath(basePath));

            committedEndOffset = endOffset;
        }

        private void flushErrorState() throws IOException {
            if (errorStatusReachedViaFenceEvent) {
                flushWritableState();
            } else {
                // We reach here only when when the in-memory state has reached TierPartitionStatus.ERROR,
                // due to an unprecedented error. In such a case it could be possible that the mutable file
                // is corrupted. Therefore, here we do not want to flush the mutable file, because we may
                // end up corrupting the persistent state. Instead we only update the header of the existing
                // flushed file with the status: TierPartitionStatus.ERROR and error event metadata. This is
                // to ensure that error status and error event metadata are persisted across restarts.
                flushHeaderWithErrorStatus();
                backupState(topicIdPartition, basePath, errorFilePath(basePath));
            }
        }

        private void flushHeaderWithErrorStatus() throws IOException {
            Path flushedFilePathHandle = flushedFilePath(basePath);
            Path tmpFilePathHandle = tmpFilePath(basePath);
            if (!Files.exists(flushedFilePathHandle)) {
                log.warn(
                        "Flushed file absent, creating empty file for {}: {}",
                        topicIdPartition, flushedFilePathHandle);
                Files.createFile(flushedFilePathHandle);
            }
            Files.copy(flushedFilePathHandle, tmpFilePathHandle, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel
                    .open(tmpFilePathHandle, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                Optional<Header> existingHeaderOpt = readHeader(channel);
                Header newHeader;
                if (existingHeaderOpt.isPresent()) {
                    Header existingHeader = existingHeaderOpt.get();
                    newHeader = new Header(
                            existingHeader.topicId(),
                            (byte) existingHeader.version(),
                            existingHeader.tierEpoch(),
                            TierPartitionStatus.ERROR,
                            existingHeader.startOffset(),
                            existingHeader.endOffset(),
                            existingHeader.globalMaterializedOffsetAndEpoch(),
                            existingHeader.localMaterializedOffsetAndEpoch(),
                            errorOffsetAndEpoch,
                            existingHeader.restoreOffsetAndEpoch());
                    log.warn("Writing new header to tier partition state for {}: {}", topicIdPartition, newHeader);
                } else {
                    newHeader = new Header(
                            topicIdPartition.topicId(),
                            version,
                            -1,
                            TierPartitionStatus.ERROR,
                            -1L,
                            -1L,
                            OffsetAndEpoch.EMPTY,
                            OffsetAndEpoch.EMPTY,
                            OffsetAndEpoch.EMPTY,
                            errorOffsetAndEpoch);
                    log.warn("Header not found! Writing new header to tier partition state for {}: {}", topicIdPartition, newHeader);
                    channel.truncate(0);
                }
                writeHeader(channel, newHeader);
                channel.force(true);
                Utils.atomicMoveWithFallback(tmpFilePathHandle, flushedFilePathHandle);
            }
        }

        private long appendWithSizePrefix(ByteBuffer metadataBuffer) throws IOException {
            final long byteOffset = channel.position();
            final int remaining = metadataBuffer.remaining();
            final short sizePrefix = (short) remaining;
            if (sizePrefix != remaining)
                throw new IllegalStateException(String.format("Unexpected metadataBuffer size: %d", remaining));
            final ByteBuffer sizeBuf = ByteBuffer.allocate(ENTRY_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            sizeBuf.putShort(0, sizePrefix);
            Utils.writeFully(channel, sizeBuf);
            Utils.writeFully(channel, metadataBuffer);
            return byteOffset;
        }

        /**
         * TierObjectMetadataIterator provides a way iterate segments with supplied
         * base offsets over a State. The TierObjectMetadataIterator is created for a given
         * state, and can thus be returned to external code when a consistent and lazy scan of metadata
         * is required by the caller. This mechanism allows State objects to be swapped in
         * the outer class during state restoration/recovery.
         */
        private class TierObjectMetadataIterator extends AbstractIterator<TierObjectMetadata> {
            final Iterator<Long> baseOffsets;

            TierObjectMetadataIterator(Iterator<Long> baseOffsets) {
                this.baseOffsets = baseOffsets;
            }

            protected TierObjectMetadata makeNext() {
                    while (baseOffsets.hasNext()) {
                        final Long offset = baseOffsets.next();
                        final Optional<TierObjectMetadata> metadata;
                        try {
                            metadata = metadata(offset);
                        } catch (IOException e) {
                            throw new KafkaStorageException(
                                    "Encountered error during iteration at target offset " + offset, e);
                        }

                        if (metadata.isPresent())
                            return metadata.get();
                    }
                    return allDone();
            }
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

/**
 * Provides a listener facility to track when a certain leader epoch has been materialized
 */
class LeaderEpochMaterializationListener {
    private final Logger log;
    private final TopicIdPartition topicIdPartition;
    private final CompletableFuture<Optional<TierObjectMetadata>> promise;
    private final int leaderEpochToMaterialize;

    LeaderEpochMaterializationListener(Logger log,
                                       TopicIdPartition topicIdPartition,
                                       int leaderEpochToMaterialize) {
        this.log = log;
        this.topicIdPartition = topicIdPartition;
        this.leaderEpochToMaterialize = leaderEpochToMaterialize;
        this.promise = new CompletableFuture<>();
    }

    CompletableFuture<Optional<TierObjectMetadata>> promise() {
        return promise;
    }

    synchronized void complete(TierObjectMetadata lastFlushedSegment) {
        if (!promise.isDone()) {
            log.info("Completing {} successfully.", this);
            promise.complete(Optional.ofNullable(lastFlushedSegment));
        }
    }

    synchronized void completeExceptionally(Exception e) {
        if (!promise.isDone()) {
            log.info("Completing {} exceptionally", this, e);
            promise.completeExceptionally(e);
        }
    }

    boolean canComplete(int tierEpoch) {
        return tierEpoch >= leaderEpochToMaterialize;
    }

    @Override
    public String toString() {
        return "LeaderEpochMaterializationListener(" +
            "topicIdPartition: " + topicIdPartition + ", " +
            "leaderEpochToMaterialize: " + leaderEpochToMaterialize + ")";
    }
}

class ReplicationMaterializationListener {
    private final Logger log;
    private final TopicIdPartition topicIdPartition;
    private final CompletableFuture<TierObjectMetadata> promise;
    private final long offsetToMaterialize;

    ReplicationMaterializationListener(Logger log,
                                       TopicIdPartition topicIdPartition,
                                       long offsetToMaterialize) {
        this.log = log;
        this.topicIdPartition = topicIdPartition;
        this.offsetToMaterialize = offsetToMaterialize;
        this.promise = new CompletableFuture<>();
    }

    Future<TierObjectMetadata> promise() {
        return promise;
    }

    synchronized void complete(TierObjectMetadata lastFlushedSegment) {
        if (!promise.isDone()) {
            log.info("Completing {} successfully. lastFlushedSegment: {}.", this, lastFlushedSegment);
            promise.complete(lastFlushedSegment);
        }
    }

    long offsetToMaterialize() {
        return offsetToMaterialize;
    }

    synchronized void completeExceptionally(Exception e) {
        if (!promise.isDone()) {
            log.info("Completing {} exceptionally", this, e);
            promise.completeExceptionally(e);
        }
    }

    boolean canComplete(TierObjectMetadata lastMaterializedSegment) {
        return lastMaterializedSegment.endOffset() >= offsetToMaterialize;
    }

    @Override
    public String toString() {
        return "ReplicationMaterializationListener(" +
                "topicIdPartition: " + topicIdPartition + ", " +
                "offsetToMaterialize: " + offsetToMaterialize + ")";
    }
}
