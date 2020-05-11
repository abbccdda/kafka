/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.server.LogDirFailureChannel;
import kafka.tier.state.OffsetAndEpoch;
import kafka.tier.state.TierPartitionState;
import kafka.tier.topic.TierTopicManagerConfig;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static scala.compat.java8.JFunction.func;

public class TierTopicManagerCommitter {
    // Version 0: Initial version
    // Version 1: Adds `leader_epoch` for each committed offset
    public static final VersionInfo CURRENT_VERSION = VersionInfo.VERSION_1;

    private static final Logger log = LoggerFactory.getLogger(TierTopicManagerCommitter.class);
    private static final int NO_EPOCH = -1;
    private static final String SEPARATOR = " ";

    private final TierTopicManagerConfig config;
    private final LogDirFailureChannel logDirFailureChannel;
    private final Map<Integer, OffsetAndEpoch> partitionToPosition = new ConcurrentHashMap<>();

    /**
     * Instantiate a TierTopicManagerCommitter
     *
     * @param config               TierTopicManagerConfig containing tiering configuration
     * @param logDirFailureChannel Log dir failure channel
     */
    public TierTopicManagerCommitter(TierTopicManagerConfig config, LogDirFailureChannel logDirFailureChannel) {
        if (config.logDirs.size() != 1)
            throw new UnsupportedOperationException("TierTopicManager does not currently support multiple logdirs.");

        this.config = config;
        this.logDirFailureChannel = logDirFailureChannel;
        clearTempFiles();
        loadOffsets();
    }

    /**
     * Update position materialized by the TierTopicManager consumer.
     *
     * @param partition Tier Topic partitionId
     * @param updateTo Tier Topic Partition position and epoch
     */
    public void updatePosition(int partition, OffsetAndEpoch updateTo) {
        OffsetAndEpoch current = partitionToPosition.getOrDefault(partition, OffsetAndEpoch.EMPTY);

        if (current.offset() >= updateTo.offset())
            throw new IllegalStateException("Illegal offset in " + updateTo + " with current position " + current);

        if (current.epoch().isPresent() && updateTo.epoch().isPresent()) {
            if (current.epoch().get() > updateTo.epoch().get())
                throw new IllegalStateException("Illegal epoch in " + updateTo + " with current position " + current);
        }

        partitionToPosition.put(partition, updateTo);
        log.debug("Committer position updated {}:{}", partition, updateTo);
    }

    /**
     * Return the current position and epoch for the given tier topic partition.
     * @param partitionId tier topic partition id
     * @return OffsetAndEpoch for the given tier topic partition; null if no position exists
     */
    public OffsetAndEpoch positionFor(int partitionId) {
        return partitionToPosition.get(partitionId);
    }

    /**
     * Flush TierPartition files to disk and then write consumer offsets to disk.
     */
    @SuppressWarnings("deprecation")
    public synchronized void flush(Iterator<TierPartitionState> tierPartitionStateIterator) {
        // take a copy of the positions so that we don't commit positions later than what we will flush.
        Map<Integer, OffsetAndEpoch> flushPositions = new HashMap<>(partitionToPosition);
        boolean error = false;
        while (tierPartitionStateIterator.hasNext()) {
            TierPartitionState state = tierPartitionStateIterator.next();
            try {
                state.flush();
            } catch (IOException ioe) {
                error = true;
                log.error("Error committing progress or flushing TierPartitionStates.", ioe);
                String logDir = state.dir().getParent();
                logDirFailureChannel.maybeAddOfflineLogDir(logDir,
                        func(() -> "Failed to flush TierPartitionState for " + state.dir()), ioe);
            }
        }

        // if any TierPartitionState files failed to flush, it is not safe
        // to write the consumer positions, as that may lead to divergence
        if (!error)
            writeOffsets(CURRENT_VERSION, flushPositions);
    }

    /**
     * Compute the earliest offsets committed across logdirs.
     *
     * @param diskOffsets list of offset mappings read from logdirs
     * @return earliest offset for each partition
     */
    static Map<Integer, OffsetAndEpoch> earliestOffsets(List<Map<Integer, OffsetAndEpoch>> diskOffsets) {
        // some positions were missing from one of the offset file
        // reset all of the positions to force full materialization
        if (diskOffsets.stream().map(Map::keySet).collect(Collectors.toSet()).size() != 1)
            return Collections.emptyMap();

        Map<Integer, OffsetAndEpoch> minimum = new HashMap<>();

        for (Map<Integer, OffsetAndEpoch> offsets : diskOffsets) {
            log.debug("Loading offsets from logdir {}.", diskOffsets);
            for (Map.Entry<Integer, OffsetAndEpoch> entry : offsets.entrySet()) {
                minimum.compute(entry.getKey(), (k, v) -> {
                    if (v == null || entry.getValue().offset() < v.offset()) {
                        return entry.getValue();
                    } else {
                        return v;
                    }
                });
            }
        }
        log.debug("Minimum offsets found {}.", minimum);
        return minimum;
    }

    private static String commitPath(String logDir) {
        return logDir + "/tier.offsets";
    }

    private static String commitTempFilename(String logDir) {
        return commitPath(logDir) + ".tmp";
    }

    @SuppressWarnings("deprecation")
    private void clearTempFiles() {
        for (String logDir : config.logDirs) {
            try {
                Files.deleteIfExists(Paths.get(commitTempFilename(logDir)));
            } catch (IOException ioe) {
                logDirFailureChannel.maybeAddOfflineLogDir(logDir,
                        func(() -> "Failed to delete temporory tier offsets in logdir."), ioe);
            }
        }
    }

    @SuppressWarnings("deprecation")
    static Map<Integer, OffsetAndEpoch> committed(String logDir, LogDirFailureChannel logDirFailureChannel) {
        try (FileReader fr = new FileReader(commitPath(logDir))) {
            try (BufferedReader br = new BufferedReader(fr)) {
                String versionLine = br.readLine();
                return readPayload(br, readVersion(versionLine));
            }
        } catch (FileNotFoundException fnf) {
            log.info("TierTopicManager offsets not found. This is expected if this is the first time starting up " +
                    "with tiered storage.", fnf);
        } catch (IOException ioe) {
            log.error("Error loading TierTopicManager offsets. Setting logdir offline.", ioe);
            logDirFailureChannel.maybeAddOfflineLogDir(logDir,
                    func(() -> "Failed to commit tier offsets to logdir."), ioe);
        } catch (Exception e) {
            log.warn("Exception encountered when reading tier checkpoint file. Resetting offsets.", e);
        }
        return Collections.emptyMap();
    }

    private static VersionInfo readVersion(String line) {
        return VersionInfo.toVersionInfo(Integer.parseInt(line));
    }

    private static Map<Integer, OffsetAndEpoch> readPayload(BufferedReader br, VersionInfo versionInfo) throws IOException {
        Map<Integer, OffsetAndEpoch> committedPositions = new HashMap<>();
        String line;

        while ((line = br.readLine()) != null) {
            String[] values = line.split(SEPARATOR);

            if (values.length == versionInfo.numFields) {
                int partitionId = Integer.parseInt(values[0]);
                long offset = Long.parseLong(values[1]);
                Optional<Integer> epoch = Optional.empty();

                if (versionInfo.version > VersionInfo.VERSION_0.version) {
                    int deserializedEpoch = Integer.parseInt(values[2]);
                    if (deserializedEpoch != NO_EPOCH)
                        epoch = Optional.of(deserializedEpoch);
                }

                OffsetAndEpoch previousPosition = committedPositions.put(partitionId, new OffsetAndEpoch(offset, epoch));
                if (previousPosition != null)
                    throw new IllegalStateException("Found duplicate positions for partition " + partitionId);
            } else {
                throw new IllegalStateException("Committed offsets found in incorrect format on line " + line);
            }
        }

        return committedPositions;
    }

    private void loadOffsets() {
        Map<Integer, OffsetAndEpoch> earliest = earliestOffsets(
                config.logDirs
                        .stream()
                        .map(logDir -> committed(logDir, logDirFailureChannel))
                        .collect(Collectors.toList()));
        partitionToPosition.clear();
        partitionToPosition.putAll(earliest);
    }

    // package-private for testing
    @SuppressWarnings("deprecation")
    void writeOffsets(VersionInfo versionInfo, Map<Integer, OffsetAndEpoch> offsets) {
        for (String logDir : config.logDirs) {
            try {
                try (FileWriter fw = new FileWriter(commitTempFilename(logDir))) {
                    try (BufferedWriter bw = new BufferedWriter(fw)) {
                        bw.write(String.valueOf(versionInfo.version));
                        bw.newLine();
                        for (Map.Entry<Integer, OffsetAndEpoch> entry : offsets.entrySet()) {
                            int partitionId = entry.getKey();
                            long offset = entry.getValue().offset();
                            int epoch = entry.getValue().epoch().orElse(NO_EPOCH);

                            bw.write(partitionId + SEPARATOR + offset);

                            // epoch is written out for version 1 and onwards
                            if (versionInfo.version > VersionInfo.VERSION_0.version)
                                bw.write(SEPARATOR + epoch);
                            bw.newLine();
                        }
                    }
                }
                Utils.atomicMoveWithFallback(Paths.get(commitTempFilename(logDir)),
                        Paths.get(commitPath(logDir)));
            } catch (IOException ioe) {
                logDirFailureChannel.maybeAddOfflineLogDir(logDir,
                        func(() -> "Failed to commit tier offsets to logdir."), ioe);
            }
        }
    }

    // package-private for tests
    enum VersionInfo {
        VERSION_0(0, 2),
        VERSION_1(1, 3);

        private static final Map<Integer, VersionInfo> VERSION_MAP = new HashMap<>();

        final int version;
        final int numFields;

        static {
            for (VersionInfo versionInfo : VersionInfo.values()) {
                VersionInfo oldVersion = VERSION_MAP.put(versionInfo.version, versionInfo);
                if (oldVersion != null)
                    throw new ExceptionInInitializerError("Found duplicate version " + versionInfo.version);
            }
        }

        VersionInfo(int version, int numFields) {
            this.version = version;
            this.numFields = numFields;
        }

        public static VersionInfo toVersionInfo(int version) {
            VersionInfo versionInfo = VERSION_MAP.get(version);
            if (versionInfo == null)
                throw new IllegalStateException("Unknown version " + version);
            return versionInfo;
        }
    }
}
