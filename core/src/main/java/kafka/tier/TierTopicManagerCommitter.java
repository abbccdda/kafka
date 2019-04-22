/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

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

public class TierTopicManagerCommitter implements Runnable {
    private static final String SEPARATOR = " ";
    private static final Logger log = LoggerFactory.getLogger(TierTopicManager.class);
    private final long commitIntervalMs;
    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch managerShutdownLatch;
    private final TierTopicManagerConfig config;
    private final ConcurrentHashMap<Integer, Long> positions = new ConcurrentHashMap<>();
    private final TierMetadataManager tierMetadataManager;

    /**
     * Instantiate a TierTopicManagerCommitter
     *
     * @param config               TierTopicManagerConfig containing tiering configuration
     * @param tierMetadataManager  Tier metadata manager instance
     * @param managerShutdownLatch Shutdown latch to signal to the TierTopicManager that it's safe to shutdown
     * @throws IOException occurs for log failures
     */
    TierTopicManagerCommitter(TierTopicManagerConfig config,
                              TierMetadataManager tierMetadataManager,
                              CountDownLatch managerShutdownLatch) throws IOException {
        this.config = config;
        this.tierMetadataManager = tierMetadataManager;
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
        try {
            // take a copy of the positions so that we don't commit positions later than what we will flush.
            HashMap<Integer, Long> flushPositions = new HashMap<>(positions);
            Iterator<TierPartitionState> metadataIterator = tierMetadataManager.tierEnabledPartitionStateIterator();
            while (metadataIterator.hasNext())
                metadataIterator.next().flush();

            writeOffsets(flushPositions);
        } catch (IOException ioe) {
            log.error("Error committing progress or flushing TierPartitionStates.", ioe);
            System.exit(1);
        }
    }

    /**
     * Close TierTopicManagerResources such as TierPartitions.
     * Finally write offsets to disk.
     */
    private void closeResources() {
        log.info("Closing tier partition resources.");
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

    private void clearTempFiles() throws IOException {
        for (String logDir : config.logDirs) {
            Files.deleteIfExists(Paths.get(commitTempFilename(logDir)));
        }
    }

    private static Map<Integer, Long> committed(String logDir) {
        HashMap<Integer, Long> loaded = new HashMap<>();
        try (FileReader fr = new FileReader(commitPath(logDir))) {
            try (BufferedReader br = new BufferedReader(fr)) {
                String line = br.readLine();
                while (line != null) {
                    try {
                        String[] values = line.split(SEPARATOR);
                        if (values.length > 2) {
                            log.warn("TierTopicManager offsets found in incorrect format. "
                                    + "Ignoring line {}.", line);
                        } else {
                            loaded.put(Integer.parseInt(values[0]), Long.parseLong(values[1]));
                        }
                    } catch (NumberFormatException nfe) {
                        log.error("Error parsing TierTopicManager offsets. Ignoring line {}.",
                                line, nfe);
                    }
                    line = br.readLine();
                }
            }
        } catch (FileNotFoundException fnf) {
            log.info("TierTopicManager offsets not found. Expected if this is the first time "
                    + "starting up with tiered storage.", fnf);
        } catch (IOException ioe) {
            log.error("Error loading TierTopicManager offsets. Ignoring.", ioe);
        }
        return loaded;
    }

    private void loadOffsets() {
        Map<Integer, Long> earliest = earliestOffsets(
                config.logDirs
                        .stream()
                        .map(TierTopicManagerCommitter::committed)
                        .collect(Collectors.toList()));
        positions.clear();
        positions.putAll(earliest);
    }

    private void writeOffsets(Map<Integer, Long> offsets) throws IOException {
        for (String logDir : config.logDirs) {
            try (FileWriter fw = new FileWriter(commitTempFilename(logDir))) {
                try (BufferedWriter bw = new BufferedWriter(fw)) {
                    for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
                        bw.write(entry.getKey() + SEPARATOR + entry.getValue());
                        bw.newLine();
                    }
                }
            }
            Utils.atomicMoveWithFallback(Paths.get(commitTempFilename(logDir)),
                    Paths.get(commitPath(logDir)));
        }
    }
}
