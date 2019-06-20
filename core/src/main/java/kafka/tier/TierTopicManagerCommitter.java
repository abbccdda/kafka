/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.server.LogDirFailureChannel;
import kafka.tier.state.TierPartitionState;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static scala.compat.java8.JFunction.func;

public class TierTopicManagerCommitter implements Runnable {
    // when bumping the format, writes in the new format should be gated behind an
    // inter broker protocol version number to allow a rollback to a previous version
    // until the version number is bumped
    static final Integer CURRENT_VERSION = 0;
    private static final String SEPARATOR = " ";
    private static final Logger log = LoggerFactory.getLogger(TierTopicManager.class);
    private final long commitIntervalMs;
    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch managerShutdownLatch;
    private final TierTopicManagerConfig config;
    private final LogDirFailureChannel logDirFailureChannel;
    private final ConcurrentHashMap<Integer, Long> positions = new ConcurrentHashMap<>();
    private final TierMetadataManager tierMetadataManager;

    /**
     * Instantiate a TierTopicManagerCommitter
     *
     * @param config               TierTopicManagerConfig containing tiering configuration
     * @param tierMetadataManager  Tier metadata manager instance
     * @param logDirFailureChannel Log dir failure channel
     * @param managerShutdownLatch Shutdown latch to signal to the TierTopicManager that it's safe to shutdown
     */
    TierTopicManagerCommitter(TierTopicManagerConfig config,
                              TierMetadataManager tierMetadataManager,
                              LogDirFailureChannel logDirFailureChannel,
                              CountDownLatch managerShutdownLatch) {
        this.config = config;
        this.tierMetadataManager = tierMetadataManager;
        this.logDirFailureChannel = logDirFailureChannel;
        this.managerShutdownLatch = managerShutdownLatch;
        this.commitIntervalMs = config.commitIntervalMs;
        if (config.logDirs.size() != 1) {
            throw new RuntimeException("TierTopicManager does not currently support multiple logdirs.");
        }
        clearTempFiles();
        loadOffsets();
    }

    /**
     * Initiate shutdown.
     */
    public void shutdown() {
        shutdownInitiated.countDown();
    }

    /**
     * Update position materialized by the TierTopicManager consumer.
     *
     * @param partition Tier Topic partitionId
     * @param position  Tier Topic Partition position
     */
    public void updatePosition(Integer partition, Long position) {
        log.debug("Committer position updated {}:{}", partition, position);
        positions.put(partition, position);
    }

    /**
     * @return the current consumer position
     */
    public ConcurrentHashMap<Integer, Long> positions() {
        return positions;
    }

    /**
     * Flush TierPartition files to disk and then write consumer offsets to disk.
     */
    public void flush() {
        // take a copy of the positions so that we don't commit positions later than what we will flush.
        HashMap<Integer, Long> flushPositions = new HashMap<>(positions);
        Iterator<TierPartitionState> metadataIterator = tierMetadataManager.tierEnabledPartitionStateIterator();
        while (metadataIterator.hasNext()) {
            TierPartitionState state = metadataIterator.next();
            try {
                state.flush();
            } catch (IOException ioe) {
                log.error("Error committing progress or flushing TierPartitionStates.", ioe);
                String logDir = state.dir().getParent();
                logDirFailureChannel.maybeAddOfflineLogDir(logDir,
                        func(() -> "Failed to flush TierPartitionState for " + state.dir()), ioe);
            }
        }

        writeOffsets(flushPositions);
    }

    /**
     * Close TierTopicManagerResources such as TierPartitions.
     * Finally write offsets to disk.
     */
    private void closeResources() {
        log.info("Closing tier committer resources.");
        try {
            // take a copy of the positions so that we don't commit positions
            // later than what we will flush.
            HashMap<Integer, Long> flushPositions = new HashMap<>(positions);
            Iterator<TierPartitionState> metadataIterator = tierMetadataManager.tierEnabledPartitionStateIterator();
            while (metadataIterator.hasNext())
                metadataIterator.next().close();
            writeOffsets(flushPositions);
        } catch (IOException ioe) {
            log.error("Error committing progress.", ioe);
        }
    }

    /**
     * Main work loop.
     */
    public void run() {
        try {
            while (!shutdownInitiated.await(commitIntervalMs, TimeUnit.MILLISECONDS))
                flush();
            // ensure we flush on shutdown
            flush();
        } catch (InterruptedException ie) {
            log.debug("Committer thread interrupted. Shutting down.");
        } finally {
            closeResources();
            managerShutdownLatch.countDown();
        }
    }

    /**
     * Compute the earliest offsets committed across logdirs.
     *
     * @param diskOffsets list of offset mappings read from logdirs
     * @return earliest offset for each partition
     */
    static Map<Integer, Long> earliestOffsets(List<Map<Integer, Long>> diskOffsets) {
        // some positions were missing from one of the offset file
        // reset all of the positions to force full materialization
        if (diskOffsets.stream().map(Map::keySet).collect(Collectors.toSet()).size() != 1)
            return new HashMap<>();

        HashMap<Integer, Long> minimum = new HashMap<>();
        for (Map<Integer, Long> offsets : diskOffsets) {
            log.debug("Loading offsets from logdir {}.", diskOffsets);
            for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
                minimum.compute(entry.getKey(), (k, v) -> {
                    if (v == null || entry.getValue() < v) {
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

    static Map<Integer, Long> committed(String logDir, LogDirFailureChannel logDirFailureChannel) {
        HashMap<Integer, Long> loaded = new HashMap<>();
        try (FileReader fr = new FileReader(commitPath(logDir))) {
            try (BufferedReader br = new BufferedReader(fr)) {
                String line = br.readLine();
                if (invalidHeader(line))
                    return new HashMap<>();

                line = br.readLine();
                while (line != null) {
                    String[] values = line.split(SEPARATOR);
                    if (values.length != 2) {
                        log.warn("TierTopicManager offsets found in incorrect format '{}'."
                                    + " Resetting positions.", line);
                        return new HashMap<>();
                    } else {
                        loaded.put(Integer.parseInt(values[0]), Long.parseLong(values[1]));
                    }
                    line = br.readLine();
                }
            }
        } catch (FileNotFoundException fnf) {
            log.info("TierTopicManager offsets not found. This is expected if this is the first "
                    + "time starting up with tiered storage.");
        } catch (NumberFormatException nfe) {
            log.error("Error parsing TierTopicManager offsets. Ignoring stored positions.", nfe);
            return new HashMap<>();
        } catch (IOException ioe) {
            log.error("Error loading TierTopicManager offsets. Setting logdir offline.", ioe);
            logDirFailureChannel.maybeAddOfflineLogDir(logDir,
                    func(() -> "Failed to commit tier offsets to logdir."), ioe);
        }
        return loaded;
    }

    private static boolean invalidHeader(String line) {
        try {
            Integer version = Integer.parseInt(line);
            if (version > CURRENT_VERSION || version < 0) {
                log.error("Committed offsets version {} is unsupported. Current version {}."
                                + " Returning empty positions.",
                        version,
                        CURRENT_VERSION);
                return true;
            }
        } catch (NumberFormatException nfe) {
            log.error("Error parsing committed offset version, line '{}'."
                         + " Returning empty positions.", line);
            return true;
        }
        return false;
    }

    private void loadOffsets() {
        Map<Integer, Long> earliest = earliestOffsets(
                config.logDirs
                        .stream()
                        .map(logDir -> committed(logDir, logDirFailureChannel))
                        .collect(Collectors.toList()));
        positions.clear();
        positions.putAll(earliest);
    }

    private void writeOffsets(Map<Integer, Long> offsets) {
        for (String logDir : config.logDirs) {
            try {
                try (FileWriter fw = new FileWriter(commitTempFilename(logDir))) {
                    try (BufferedWriter bw = new BufferedWriter(fw)) {
                        bw.write(CURRENT_VERSION.toString());
                        bw.newLine();
                        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
                            bw.write(entry.getKey() + SEPARATOR + entry.getValue());
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
}
