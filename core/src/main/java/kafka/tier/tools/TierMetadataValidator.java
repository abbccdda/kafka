/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import kafka.log.Log;
import kafka.server.KafkaConfig;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierObjectMetadata.State;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import kafka.tier.fetcher.CancellationContext;
import kafka.tier.fetcher.TierSegmentReader;
import kafka.tier.state.FileTierPartitionIterator;
import kafka.tier.state.FileTierPartitionState;
import kafka.tier.state.Header;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStore.Backend;
import kafka.tier.store.TierObjectStore.FileType;
import kafka.tier.store.TierObjectStore.ObjectMetadata;
import kafka.tier.store.TierObjectStoreConfig;
import kafka.tier.store.TierObjectStoreResponse;
import kafka.tier.store.TierObjectStoreUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

/**
 * TierMetadataValidator: Tool to validate the materialization of the tier topic partition's materialization states. The
 * approach is that for given kafka data path, the tool will snapshot all the TierPartition state's file to its working
 * directory and then after computing the map of topic partition to max consumer tier topic partition's offset, the
 * expected state is computed after consuming the relevant tier topic partition's events. Finally the expected state is
 * compared against the snapshot one.
 *
 * Usage
 * # Validate all the local state file on the existing broker with optional workdir and path to metadata-states-dir.
 * $KAFKA_BIN/kafka-tier-materialization-validator.sh --metadata-states-dir=<kafka-data-path> --working-dir=/tmp/work-dir
 *
 * TBD : Support flag to directly validate state files in given snapshot folder. This will be used for periodical
 * validator/scrubber which may run from separate system and may call this after collecting all state files.
 */
public class TierMetadataValidator implements AutoCloseable {
    private Map<TopicIdPartition, TierMetadataValidatorRecord> stateMap = new HashMap<>();
    public final String metadataStatesDir;
    public final String workDir;
    private static final String SNAPSHOT_DIR_SUFFIX = "snapshots";
    public Properties props;
    private TierObjectStore objectStore;
    private boolean validateAgainstObjectStore;
    private boolean verifyOffsetScanAgainstObjectStore = false;
    private Backend backend = null;
    // Use utils to materialize various tier state file. This is initialized in the run() method.
    TierTopicMaterializationUtils utils;
    // The following args are specific to validation against the S3 object store
    private static final int OBJECT_STORE_RETRY_COUNT = 3;
    private static final long OBJECT_STORE_BACKOFF_MS = 1000L;
    private final CancellationContext cancellationContext;
    private static final String OFFSET_SCAN_PREFIX = "[OFFSET_SCAN] ";

    TierMetadataValidator(String[] args) {
        props = new Properties();
        parseArgs(args);
        this.workDir = props.getProperty(TierTopicMaterializationToolConfig.WORKING_DIR);
        this.metadataStatesDir = props.getProperty(TierTopicMaterializationToolConfig.METADATA_STATES_DIR);
        this.validateAgainstObjectStore = (boolean) props.get(TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION);
        if (this.validateAgainstObjectStore) {
            this.backend = (Backend) props.get(KafkaConfig.TierBackendProp());
            this.verifyOffsetScanAgainstObjectStore = (boolean) props.get(TierTopicMaterializationToolConfig.TIER_STORAGE_OFFSET_VALIDATION);
            if (this.verifyOffsetScanAgainstObjectStore && this.backend != Backend.S3) {
                throw new IllegalArgumentException("Unsupported backend for offset scan: " + this.backend);
            }
            TierObjectStoreConfig objectStoreConfig = TierObjectStoreUtils.generateBackendConfig(this.backend, props);
            this.objectStore = TierObjectStoreFactory.getObjectStoreInstance(this.backend, objectStoreConfig);
            System.out.println("Successfully created backend: " + this.backend);
        }
        this.cancellationContext = CancellationContext.newContext();
    }

    private void parseArgs(String[] args) {
        OptionParser parser = new OptionParser();

        OptionSpec<String> workingDirSpec = parser.accepts(TierTopicMaterializationToolConfig.WORKING_DIR,
                TierTopicMaterializationToolConfig.WORKING_DIR_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.WORKING_DIR)
                .ofType(String.class)
                .defaultsTo("/tmp/workdir");
        OptionSpec<String> metaStatesDirSpec = parser.accepts(TierTopicMaterializationToolConfig.METADATA_STATES_DIR,
                TierTopicMaterializationToolConfig.METADATA_STATES_DIR_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.METADATA_STATES_DIR)
                .ofType(String.class);
        OptionSpec<String> bootStrapServerSpec = parser.accepts(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG,
                TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_DOC)
                .ofType(String.class)
                .defaultsTo("localhost:9092");
        OptionSpec<Integer> tierSytateTopicPartitionSpec = parser.accepts(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION,
                TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION_DOC)
                .ofType(Integer.class)
                .defaultsTo(-1);
        OptionSpec<Boolean> dumpEventsSpec = parser.accepts(TierTopicMaterializationToolConfig.DUMP_EVENTS,
                TierTopicMaterializationToolConfig.DUMP_EVENTS_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.DUMP_EVENTS_DOC)
                .ofType(Boolean.class)
                .defaultsTo(true);
        OptionSpec<Boolean> snapshotStatesSpec = parser.accepts(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES,
                TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES_DOC)
                .ofType(Boolean.class)
                .defaultsTo(true);

        TierCloudBackendUtils.augmentParserWithValidatorOpts(parser);
        TierCloudBackendUtils.augmentParserWithTierBackendOpts(parser);

        OptionSet options = parser.parse(args);

        // Only one of metadata-states-dir and snapshot-states should be opted.
        if (options.hasArgument(metaStatesDirSpec) ^ options.valueOf(snapshotStatesSpec)) {
            throw new IllegalStateException("Only one of " + TierTopicMaterializationToolConfig.METADATA_STATES_DIR
                + " or " + TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES + " should be specified.");
        }
        if (options.hasArgument(metaStatesDirSpec))
            props.put(TierTopicMaterializationToolConfig.METADATA_STATES_DIR, options.valueOf(metaStatesDirSpec));
        props.put(TierTopicMaterializationToolConfig.WORKING_DIR, options.valueOf(workingDirSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_METADATA, "true");
        props.put(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG, options.valueOf(bootStrapServerSpec));
        props.put(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION, options.valueOf(tierSytateTopicPartitionSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_EVENTS, options.valueOf(dumpEventsSpec));
        props.put(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES, options.valueOf(snapshotStatesSpec));

        TierCloudBackendUtils.addValidatorProps(options, props);
        TierCloudBackendUtils.addTierBackendProps(options, props);

        System.out.println("Starting Validation with following args " + props);
    }

    private void createWorkDir(String dir) {
        File file = new File(dir);
        File snapshotDir = new File(getSnapshotDir(dir));
        if (this.props.get(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES).equals(false)) {
            // We expect workDir and snapshot directory already created with snapshot directory
            // populated with state files.
            if (!file.exists() || !snapshotDir.exists()) {
                throw new IllegalStateException(dir + " and " + snapshotDir.toPath() + " should exist.");
            }
            return;
        }

        if (!file.exists())
            file.mkdirs();
        if (!file.isDirectory() || file.listFiles().length != 0) {
            throw new IllegalStateException("materialization-path needs to be directory and should be empty");
        }

        if (!snapshotDir.exists())
            snapshotDir.mkdir();
        if (!snapshotDir.isDirectory() || snapshotDir.listFiles().length != 0) {
            throw new IllegalStateException("snapshot path " + snapshotDir.getAbsolutePath() + " needs to be "
                + "directory and should be empty");
        }
    }

    static String getSnapshotDir(String dir) {
        return Paths.get(dir, SNAPSHOT_DIR_SUFFIX).toString();
    }

    // exposed for tests
    static Path getSnapshotFilePath(TopicPartition id, String baseDir) {
        return Paths.get(baseDir, id.topic() + "-" + id.partition());
    }

    public void run() throws IOException {
        createWorkDir(this.workDir);

        System.out.println("**** Fetching target partition states from folder. \n");
        // Create snapshot of all the state files.
        final boolean skipPopulate = this.props.get(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES).equals(false);
        final File tierStateFolder;
        if (skipPopulate)
            tierStateFolder = new File(getSnapshotDir(this.workDir));
        else
            tierStateFolder = new File(props.getProperty(TierTopicMaterializationToolConfig.METADATA_STATES_DIR));
        this.stateMap = snapshotStateFiles(tierStateFolder, !skipPopulate, this.workDir);

        // Calculate the maximum offset needed.
        HashMap<TopicIdPartition, Long> offsetMap = new HashMap<>();
        for (Map.Entry<TopicIdPartition, TierMetadataValidatorRecord> entry : stateMap.entrySet()) {
            TopicIdPartition id = entry.getKey();
            offsetMap.put(id, entry.getValue().maxOffset);
        }

        System.out.println("**** Calling materialization for following partition to offset mapping " + offsetMap + " \n");
        if (offsetMap.size() != 0 && !props.get(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION).equals(-1)) {
            // We will automatically set the end offset here.
            int endOffset = Collections.max(offsetMap.values()).intValue();
            props.put(TierTopicMaterializationToolConfig.END_OFFSET, endOffset);
            System.out.println("Setting end-offset to " + endOffset);
        }

        utils = new TierTopicMaterializationUtils(new TierTopicMaterializationToolConfig(props), offsetMap);
        utils.run();

        System.out.println("**** Calling validator. \n");
        // Validate the state files.
        for (TopicIdPartition topicIdPartition : utils.stateMap.keySet()) {
            try {
                Path eFile = utils.getTierStateFile(topicIdPartition);
                Path aFile = stateMap.get(topicIdPartition).snapshot;
                if (validateStates(eFile, aFile, topicIdPartition.topicPartition(),
                        utils.getStartOffset(topicIdPartition.topicPartition()))) {
                    System.out.println("Metadata states is consistent " + eFile + " Vs " + aFile);
                }
            } catch (Exception ex) {
                System.out.println("Ignoring comparison for non local.");
            }
        }
    }

    /**
     * Manages the tierState files from the live broker. If 'populate' is set to true than copies the
     * tierState files in the snapshot folder, maintaining the user topic partition's data directory
     * naming convention. If 'populate' is not set than it is expected that tierState files are already
     * populated.
     * Also, process each states file by reading its header and initializing its properties like
     * lastLocalOffset etc.
     *
     * @param tierStateFolder path of snapshot folder if populate is false else path of live data log folder.
     * @param populate        if true than copy live tierState file of user partitions to snapshot folder.
     * @param workDir         current workDir to generate the snapshots directory in case we want to copy from the broker
     * @return the map containing the details of the materialized file per topicIdPartition
     * @throws IOException In case of unhandled exception we will let the exception terminate the application.
     */
    static Map<TopicIdPartition, TierMetadataValidatorRecord> snapshotStateFiles(File tierStateFolder, boolean populate,
                                                                                 String workDir) throws IOException {
        if (!tierStateFolder.isDirectory()) {
            throw new IllegalStateException(tierStateFolder + " is not metadata states directory");
        }

        Map<TopicIdPartition, TierMetadataValidatorRecord> stateMap = new HashMap<>();

        for (File dir: tierStateFolder.listFiles()) {
            if (dir.isDirectory()) {
                TopicPartition topicPartition;

                try {
                    topicPartition = Log.parseTopicPartitionName(dir);
                } catch (KafkaException ex) {
                    // Ignore directories which are not Topic partition data directory.
                    // Rest of the exceptions will be
                    // still raised.
                    continue;
                }

                // If populate is true, we would need to generate the snapshots folder from the workdir, otherwise
                // the provided tierStateFolder itself should be the snapshots folder
                File snapshotFile = populate ? getSnapshotFilePath(topicPartition, getSnapshotDir(workDir)).toFile() :
                        getSnapshotFilePath(topicPartition, tierStateFolder.getAbsolutePath()).toFile();
                if (populate) {
                    if (!snapshotFile.exists())
                        snapshotFile.mkdir();
                    System.out.println("Found TierTopicPartition dir " + dir.toPath());
                }

                for (File file: dir.listFiles()) {
                    if (file.isFile() && Log.isTierStateFile(file)) {
                        Path ss = Paths.get(snapshotFile.toString(), file.getName());
                        if (populate) {
                            System.out.println("Taking snapshot of partition states for " + topicPartition);
                            Files.copy(file.toPath(), ss);
                            System.out.println("Copied state files " + ss);
                        }
                        TierMetadataValidatorRecord record = new TierMetadataValidatorRecord(
                            ss, topicPartition);
                        stateMap.put(record.id, record);
                    }
                }
            }
        }

        if (stateMap.isEmpty()) {
            throw new IllegalStateException("Can not find any metadata states file in " + tierStateFolder);
        }
        return stateMap;
    }

    /**
     * maybeActiveState helps in identifying the metadata object 'metadata' to be currently in used
     * for fetching the records. The 'startOffset' represents the log's starting offset.
     * There can be scenario when 'startOffset' may not be available -say verifying from recorded states,
     * the method will find the first metadata state which represents an active mapping.
     */
    private static boolean mayBeActiveObject(long startOffset, TierObjectMetadata metadata) {
        List<State> inActiveStateList = ImmutableList.of(State.SEGMENT_FENCED,
            State.SEGMENT_DELETE_COMPLETE, State.SEGMENT_DELETE_INITIATE, State.SEGMENT_UPLOAD_INITIATE);
        List<State> nonCommittedList = ImmutableList.of(State.SEGMENT_FENCED);
        if (startOffset != TierTopicMaterializationToolConfig.UNKNOWN_OFFSET) {
            return startOffset <= metadata.endOffset() && !nonCommittedList.contains(metadata.state());
        }
        return !inActiveStateList.contains(metadata.state());
    }

    /**
     * For the given 'expected' and 'actual' metadata states path, the method will validate if they
     * are same or not. It also checks to see if the states represented by the metadata states
     * file are valid or not (basically check for any offset range).
     */
    public boolean validateStates(Path expected, Path actual, TopicPartition id, long startOffset) throws IOException {
        FileTierPartitionIterator eiterator = getTierPartitionIterator(id, expected);
        FileTierPartitionIterator aiterator = getTierPartitionIterator(id, actual);

        Optional<TierObjectStore> objectStoreOpt = Optional.ofNullable(objectStore);

        if (!comparesStates(actual, expected) || !isValidStates(eiterator, aiterator, startOffset, objectStoreOpt,
                verifyOffsetScanAgainstObjectStore, cancellationContext, utils::getStartOffset)) {
            System.out.println("Metadata inconsistencies(" + id + ") " + actual + "Vs " + expected);
            return false;
        }
        return true;
    }

    /**
     * This is a utility function through the TierMetadataValidator will make sure whether the provided tierstate file
     * has any inconsistencies in it.
     * The inconsistencies could be of various types, including non-contiguous offset ranges or unexpected offset gap
     * found in the remote segment pointed to by the tierstate file.
     * However, to make sure that the validator has a way to ignore such issues for inactive offsets, the user would
     * also have to provide a startOffsetProducer which would be used to check for startOffset changes.
     * For tests, it's okay to provide a constant producer such as
     * Function<TopicPartition, Long> startOffsetProducer = topic -> 0L;
     *
     * @param tierStateFile       the Path to the .tierstate to be verified
     * @param id                  topicIdPartition that is described by the tierstate file
     * @param objStoreOpt         optional instance of the backend object store, if defined, then we should verify object presence
     * @param verifyOffsetScan    boolean flag to indicate if we need to scan the offsets for inconsistencies in the remote segment files
     * @param cancellationContext used for cancelling long-running verification tasks in case the tool is shutting down
     * @param startOffsetProducer lambda used to fetch the latest firstValidOffset in case we see missing segment files
     * @return validatorResult the validation result object
     * @throws IOException This may be thrown due to accessing the tierstate file from local disk
     */
    static TierMetadataValidatorResult validateStandaloneTierStateFile(Path tierStateFile, TopicIdPartition id,
                                                                       Optional<TierObjectStore> objStoreOpt,
                                                                       boolean verifyOffsetScan,
                                                                       CancellationContext cancellationContext,
                                                                       Function<TopicPartition, Long> startOffsetProducer) throws IOException {
        long currentStartOffset = startOffsetProducer.apply(id.topicPartition());
        Path copiedTierStateFile = Paths.get(tierStateFile.toUri());
        FileTierPartitionIterator expIterator = TierMetadataValidator.getTierPartitionIterator(id.topicPartition(), tierStateFile);
        FileTierPartitionIterator actIterator = getTierPartitionIterator(id.topicPartition(), copiedTierStateFile);
        boolean isValid = isValidStates(expIterator, actIterator, currentStartOffset, objStoreOpt, verifyOffsetScan,
                cancellationContext, startOffsetProducer);
        final Optional<Header> headerOpt = FileTierPartitionState.readHeader(
                FileChannel.open(tierStateFile, StandardOpenOption.READ));
        if (isValid && headerOpt.isPresent()) {
            System.out.println("Metadata state is consistent for file: " + tierStateFile);
        } else {
            System.out.println("Metadata state is inconsistent for file: " + tierStateFile);
        }
        return new TierMetadataValidatorResult(isValid, headerOpt);
    }

    static FileTierPartitionIterator getTierPartitionIterator(TopicPartition id, Path tierstateFile) throws IOException {
        FileChannel channel = FileChannel.open(tierstateFile, StandardOpenOption.READ);
        return FileTierPartitionState.iterator(id, channel).orElseGet(() -> {
            throw new IllegalStateException("Couldn't create tierPartitionIterator for: " + id + " from file: " + tierstateFile);
        });
    }

    /**
     * Compares the 'actual' state file with 'expected' state file. Initially we do header check and
     * than we do check if the files are identical at byte level or not.
     */
    public boolean comparesStates(Path actual, Path expected) throws IOException {
        FileChannel echannel = FileChannel.open(actual, StandardOpenOption.READ);
        FileChannel achannel = FileChannel.open(expected, StandardOpenOption.READ);

        Header eheader = FileTierPartitionState.readHeader(echannel).get();
        Header aheader = FileTierPartitionState.readHeader(achannel).get();

        // Header comparison report helps in quick debugging, hence doing explicit header check before
        // doing byte level comparison.
        if (!eheader.equals(aheader)) {
            System.err
                .println("Metadata states(header) inconsistency " + eheader + " Vs " + aheader);
            return false;
        }

        byte[] f1 = Files.readAllBytes(expected);
        byte[] f2 = Files.readAllBytes(actual);
        if (!Arrays.equals(f1, f2)) {
            System.out.println(
                "Metadata inconsistency(files do not match).");
            return false;
        }

        return true;
    }

    private static boolean objectExistsOnTierStore(TierObjectMetadata tierMetadata, TierObjectStore objStore,
                                                   boolean offsetScan, CancellationContext cancellationContext) {
        try {
            return checkObjectStoreExistenceWithRetries(tierMetadata, objStore, offsetScan, 0, cancellationContext);
        } catch (InterruptedException e) {
            System.err.println("Interrupted while retrying for checkObjectStoreExistenceWithRetries with object(" + tierMetadata + "): " + e);
            return false;
        }
    }

    private static boolean checkObjectStoreExistenceWithRetries(TierObjectMetadata tierMetadata, TierObjectStore objStore,
                                                                boolean offsetScan, int retryCount,
                                                                CancellationContext cancellationContext) throws InterruptedException {
        if (retryCount >= OBJECT_STORE_RETRY_COUNT) {
            System.err.println("checkObjectStoreExistenceWithRetries reached maximum retries #" + retryCount + " for object: " + tierMetadata);
            return false;
        }
        ObjectMetadata objMetadata = new ObjectMetadata(tierMetadata);
        try {
            TierObjectStoreResponse objStoreResponse = objStore.getObject(objMetadata, FileType.SEGMENT);
            return handleObjectStoreResponse(objStoreResponse, tierMetadata, offsetScan, objStore.getBackend(), cancellationContext);
        } catch (TierObjectStoreRetriableException e) {
            System.err.println("Received Transient error from ObjectStore: " + e.getCause() + ", will retry: " + e);
            long sleepDuration = OBJECT_STORE_BACKOFF_MS * (1 + retryCount);
            System.out.println("ObjectStore retryCount#" + retryCount + ". Going to sleep for " + sleepDuration + "ms");
            Thread.sleep(sleepDuration);
            return checkObjectStoreExistenceWithRetries(tierMetadata, objStore, offsetScan, retryCount + 1,
                    cancellationContext);
        } catch (Exception e) {
            System.err.println("ObjectStore: " + objStore + " actualObj: " + tierMetadata + " raised fatal error: " + e);
            return false;
        }
    }

    /**
     * This method performs the offset scan within the inputStream that is received from the response object.
     * The current implementation only supports S3 as the backend if offset scanning is required.
     * For S3 backend, it uses the TierSegmentReader to create a wrapper around the inputStream with associated metadata
     * Once this reader is created, it uses reader.readBatch to keep on consuming the batches to ensure that there is
     * no gap amongst the offsets of consecutive batches.
     * However, if there are gaps amongst the offsets of consecutive batches, we only log the inconsistencies and not fail
     * the scan.
     * In addition to this, this method also performs a couple of cumulative checks such as, the metadata's baseOffset
     * and endOffset are aligning with the first and last batches, and that the total cumulative bytes within all the batches
     * are equal to that of the segment size as defined in the metadata.
     * Note: the offset scanning can consume considerable of runtime and if the purpose is to only check for the object's
     * presence in the object store, then the offsetScan flag should be turned off.
     * @param response Tier object store response from which the batches will be read
     * @param object The metadata object against which the batches will be scanned
     * @param offsetScan  The boolean flag to indicate whether the offset of batches would be scanned within the response
     * @return True if the object verification went okay
     */
    private static boolean handleObjectStoreResponse(TierObjectStoreResponse response, TierObjectMetadata object,
                                                     boolean offsetScan, Backend backend, CancellationContext cancellationContext) {
        try (InputStream inputStream = response.getInputStream()) {
            if (offsetScan) {
                if (backend != Backend.S3) {
                    throw new UnsupportedOperationException(OFFSET_SCAN_PREFIX + "Unsupported Backend for offset scan: " + backend);
                }
                System.out.println(OFFSET_SCAN_PREFIX + "Beginning to perform offset scan for object: " + object);
                TierSegmentReader reader = new TierSegmentReader(OFFSET_SCAN_PREFIX);
                long cumulativeVerifiedSize = 0;
                long lastRetrievedOffset = TierTopicMaterializationToolConfig.UNKNOWN_OFFSET;
                while (!cancellationContext.isCancelled() && lastRetrievedOffset < object.endOffset()) {
                    RecordBatch batch = reader.readBatch(inputStream, object.size());
                    if (lastRetrievedOffset == TierTopicMaterializationToolConfig.UNKNOWN_OFFSET && batch.baseOffset() != object.baseOffset()) {
                        System.err.println(OFFSET_SCAN_PREFIX + "Offset mismatch between first batch offset: " + batch.baseOffset() +
                                " and metadata base offset: " + object.baseOffset() + " for object: " + object);
                        return false;
                    } else if (lastRetrievedOffset != TierTopicMaterializationToolConfig.UNKNOWN_OFFSET && batch.baseOffset() - lastRetrievedOffset != 1) {
                        // currently we are warning in case we see a discrepancy in offset continuity between record batches
                        System.err.println("Metadata inconsistency between S3 record batches: Received batch.baseOffset(): " + batch.baseOffset() +
                                " after lastRetrievedOffset: " + lastRetrievedOffset + " for object: " + object);
                    }
                    lastRetrievedOffset = batch.lastOffset();
                    cumulativeVerifiedSize += batch.sizeInBytes();
                    // The following output is muted for now because of the frequency. It can turned back on at the trace level
                    // System.out.println(s3OffsetScanPrefix + "Completed offset scan till: " + lastRetrievedOffset + ", data size (in bytes): " + cumulativeVerifiedSize);
                }
                if (cancellationContext.isCancelled()) {
                    System.out.println(OFFSET_SCAN_PREFIX + "Cancelled after verifying till: " + lastRetrievedOffset + " for object: " + object);
                }
                if (lastRetrievedOffset != object.endOffset()) {
                    System.err.println(OFFSET_SCAN_PREFIX + "Metadata inconsistency, couldn't verify till end of segment: " + lastRetrievedOffset +
                            " vs " + object.endOffset() + " for object: " + object);
                    return false;
                } else if (cumulativeVerifiedSize != object.size()) {
                    System.err.println(OFFSET_SCAN_PREFIX + "Metadata inconsistency, couldn't verify the entire bytes in the segment. ByteCount:" +
                            cumulativeVerifiedSize + " vs " + object.size() + " for object: " + object);
                    return false;
                }
            } else if (response.getInputStream().available() <= 0) {
                System.err.println("Received empty response for object: " + object);
                return false;
            }
            System.out.println(OFFSET_SCAN_PREFIX + "Successfully validated from object: " + object);
            return true;
        } catch (Exception e) {
            System.err.println(OFFSET_SCAN_PREFIX + "Encountered error while handling response for object: " + object + " with exception: " + e);
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method is a wrapper around the logic of verifying the segment files on S3 backend. In addition to verifying the object in the tier storage,
     * this method also has the following logic around active segment detection:
     * In case the object is not found on the object store after retries, this checks if the firstValidOffset has moved forward on the live server,
     * and if it detects such an update, it will update the local first offset marker as well.
     * In addition to this, it will ignore object store inconsistencies for segments that are inactive as defined in mayBeActiveObject() as well as if
     * the first valid offset is UNKNOWN_OFFSET.
     * Note: Currently we support only S3 for both object verification and offset scan
     * @param objectMetadata the tier object metadata used to fetch the object from the tier storage
     * @param firstValidOffset the first valid offset on the tier topic partition
     * @param objStore the object store instance
     * @param offsetScan boolean flag indicating whether to perform the offset scan on the segment file
     * @param startOffsetProducer a producer function that will be called to fetch the refreshed firstValidOffset
     * @return A tuple with the verification result and the firstValidOffset, to avoid further active segment lookups for the same iterator
     */
    static OffsetValidationResult verifyObjectInBackend(TierObjectMetadata objectMetadata, long firstValidOffset,
                                                        TierObjectStore objStore, boolean offsetScan,
                                                        CancellationContext cancellationContext,
                                                        Function<TopicPartition, Long> startOffsetProducer) {
        boolean objectPresentInTierStore = objectExistsOnTierStore(objectMetadata, objStore, offsetScan, cancellationContext);
        OffsetValidationResult result = new OffsetValidationResult(true, firstValidOffset);
        if (!objectPresentInTierStore) {
            long updatedFirstValidOffset = startOffsetProducer.apply(objectMetadata.topicIdPartition().topicPartition());
            boolean activeSegment = true;
            if (updatedFirstValidOffset > firstValidOffset) {
                // Here we are trying to see if the firstValidOffset has been updated in the broker and
                // update the local active state if necessary
                System.out.println("Updated firstValidOffset from: " + firstValidOffset + " to: " + updatedFirstValidOffset);
                firstValidOffset = updatedFirstValidOffset;
                activeSegment = mayBeActiveObject(firstValidOffset, objectMetadata);
                result.firstValidOffset = firstValidOffset;
            }
            // In case we are not connected to the live cluster, we will keep on getting the first offset as UNKNOWN_OFFSET
            // Hence, we would simply log such inconsistencies and continue scanning
            // Otherwise we will log the error that the object was not found in the objectStore
            if (activeSegment && updatedFirstValidOffset != TierTopicMaterializationToolConfig.UNKNOWN_OFFSET) {
                System.err.println("ObjectStore inconsistency. Object: " + objectMetadata + " not found in objectStore: " + objStore);
                result.result = false;
            } else {
                // We can't guarantee object existence in TierStore if the segment isn't active
                System.out.println("Ignoring inactive Object at offset: " + objectMetadata.baseOffset());
            }
        }
        return result;
    }

    public static boolean isValidStates(Iterator<TierObjectMetadata> eIterator, Iterator<TierObjectMetadata> aIterator,
                                        long firstValidOffset, Optional<TierObjectStore> objectStoreOpt, boolean verifyOffsetScan,
                                        CancellationContext cancellationContext,
                                        Function<TopicPartition, Long> startOffsetProducer) {
        long prevEndOffset = -1;

        while (eIterator.hasNext()) {
            if (!aIterator.hasNext()) {
                System.err.println("Metadata inconsistency(more states) for #expected > #actual");
                return false;
            }

            TierObjectMetadata expectedObject = eIterator.next();
            TierObjectMetadata actualObject = aIterator.next();
            boolean active = mayBeActiveObject(firstValidOffset, actualObject);

            if (actualObject.equals(expectedObject)) {
                long start = Math.max(expectedObject.baseOffset(), prevEndOffset + 1);
                if (actualObject.state().equals(State.SEGMENT_FENCED))
                    continue;
                if ((start - prevEndOffset != 1) || (actualObject.endOffset() <= prevEndOffset)) {
                    if (active) {
                        System.err.println("Metadata offset inconsistency "
                            + actualObject.baseOffset() + " : " + actualObject.endOffset());
                        System.out.println(actualObject);
                        return false;
                    } else {
                        // We found hole in offset range of states mappings. This is possible
                        // due to various scenario's like - retention before archival, first tiered
                        // offset etc. None of this is possible for active offsets (offset which is
                        // active for reading). Since this is inactive offset range, we will ignore.
                    }
                } else if (objectStoreOpt.isPresent() && active && actualObject.state() == State.SEGMENT_UPLOAD_COMPLETE) {
                    OffsetValidationResult result = verifyObjectInBackend(actualObject, firstValidOffset, objectStoreOpt.get(),
                            verifyOffsetScan, cancellationContext, startOffsetProducer);
                    if (result.firstValidOffset > firstValidOffset) {
                        firstValidOffset = result.firstValidOffset;
                    }
                    if (!result.result) {
                        return false;
                    }
                }
            } else {
                System.err.println("Metadata states inconsistency at " + actualObject);
                return false;
            }

            prevEndOffset = expectedObject.endOffset();
        }

        if (aIterator.hasNext()) {
            System.err.println("Metadata inconsistency(more states) for #expected < #actual");
            return false;
        }
        return true;
    }

    /**
     * This method will be primarily used to clean up resources including various backends
     */
    @Override
    public void close() {
        if (validateAgainstObjectStore && backend != null) {
            cancellationContext.cancel();
            TierObjectStoreFactory.closeBackendInstance(backend);
        }
    }

    static class TierMetadataValidatorResult {
        final boolean valid;
        final Optional<Header> headerOpt;

        public TierMetadataValidatorResult(final boolean valid, final Optional<Header> headerOpt) {
            this.valid = valid;
            this.headerOpt = headerOpt;
        }
    }

    static class TierMetadataValidatorRecord {
        public Path snapshot;
        public TopicIdPartition id;
        public long maxOffset;

        public TierMetadataValidatorRecord(Path stateFile, TopicPartition topicPartition) throws IOException {
            FileChannel fileChannel = FileChannel.open(stateFile, StandardOpenOption.READ);
            Optional<Header>  headerOpt = FileTierPartitionState.readHeader(fileChannel);
            if (!headerOpt.isPresent())
                return;
            Header header = headerOpt.get();
            this.snapshot = stateFile;
            this.id = new TopicIdPartition(topicPartition.topic(), header.topicId(), topicPartition.partition());
            this.maxOffset = header.localMaterializedOffsetAndEpoch().offset();
        }

        @Override
        public String toString() {
            return "TierMetadataValidatorRecord{" +
                    "snapshot=" + snapshot +
                    ", id=" + id +
                    ", maxOffset=" + maxOffset +
                    '}';
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Atleast " + TierTopicMaterializationToolConfig.METADATA_STATES_DIR + " needs to be set.");
            System.exit(1);
        }
        try (TierMetadataValidator validator = new TierMetadataValidator(args)) {
            validator.run();
        } catch (Exception ae) {
            System.out.println("Exception: " +  ae.getMessage());
            ae.printStackTrace();
            System.exit(1);
        }
    }

    static class OffsetValidationResult {
        boolean result;
        long firstValidOffset;

        OffsetValidationResult(boolean result, long firstValidOffset) {
            this.result = result;
            this.firstValidOffset = firstValidOffset;
        }
    }
}

