/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
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
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import kafka.log.Log;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierObjectMetadata.State;
import kafka.tier.state.FileTierPartitionIterator;
import kafka.tier.state.FileTierPartitionState;
import kafka.tier.state.Header;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

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
public class TierMetadataValidator {
    private HashMap<TopicIdPartition, TierMetadataValidatorRecord> stateMap = new HashMap<>();
    public final String metadataStatesDir;
    public final String workDir;
    private final String snapshotDirSuffix = "snapshots";
    public Properties props;

    TierMetadataValidator(String[] args) {
        props = new Properties();
        parseArgs(args);
        this.workDir = props.getProperty(TierTopicMaterializationToolConfig.WORKING_DIR);
        this.metadataStatesDir = props.getProperty(TierTopicMaterializationToolConfig.METADATA_STATES_DIR);
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

        OptionSet options = parser.parse(args);

        // Only one of metadata-states-dir and snapshot-states should be opted.
        if (options.hasArgument(metaStatesDirSpec) ^ options.valueOf(snapshotStatesSpec)) {
            System.err.println("Only one of " + TierTopicMaterializationToolConfig.METADATA_STATES_DIR
                + " or " + TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES + " should be specified.");
            System.exit(1);
        }
        if (options.hasArgument(metaStatesDirSpec))
            props.put(TierTopicMaterializationToolConfig.METADATA_STATES_DIR, options.valueOf(metaStatesDirSpec));
        props.put(TierTopicMaterializationToolConfig.WORKING_DIR, options.valueOf(workingDirSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_METADATA, "true");
        props.put(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG, options.valueOf(bootStrapServerSpec));
        props.put(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION, options.valueOf(tierSytateTopicPartitionSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_EVENTS, options.valueOf(dumpEventsSpec));
        props.put(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES, options.valueOf(snapshotStatesSpec));

        System.out.println("Starting Validation with following args " + props);
    }

    private void createWorkDir(String dir) {
        File file = new File(dir);
        File snapshotDir = new File(getSnapshotDir(dir));
        if (this.props.get(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES).equals(false)) {
            // We expect workDir and snapshot directory already created with snapshot directory
            // populated with state files.
            if (!file.exists() || !snapshotDir.exists()) {
                System.err.println(dir + " and " + snapshotDir.toPath() + " should exist.");
                System.exit(1);
            }
            return;
        }

        if (!file.exists())
            file.mkdirs();
        if (!file.isDirectory() || file.listFiles().length != 0) {
            System.err.println("materialization-path needs to be directory and should be empty");
            System.exit(1);
        }

        if (!snapshotDir.exists())
            snapshotDir.mkdir();
        if (!snapshotDir.isDirectory() || snapshotDir.listFiles().length != 0) {
            System.err.println("snapshot path " + snapshotDir.getAbsolutePath() + " needs to be "
                + "directory and should be empty");
            System.exit(1);
        }
    }

    private String getSnapshotDir(String dir) {
        return Paths.get(dir, snapshotDirSuffix).toString();
    }

    private Path getSnapshotFilePath(TopicPartition id) {
        return Paths.get(getSnapshotDir(this.workDir), id.topic() + "-" + id.partition());
    }

    public void run() throws IOException {
        createWorkDir(this.workDir);

        System.out.println("**** Fetching target partition states from folder. \n");
        // Create snapshot of all the state files.
        if (this.props.get(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES).equals(false))
            snapshotStateFiles(new File(getSnapshotDir(this.workDir)), false);
        else
            snapshotStateFiles(new File(props.getProperty(TierTopicMaterializationToolConfig.METADATA_STATES_DIR)), true);

        // Calculate the maximum offset needed.
        HashMap<TopicIdPartition, Long> offsetMap = new HashMap();
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

        // Use utils to materialize various tier state file.
        TierTopicMaterializationUtils utils = new TierTopicMaterializationUtils(
                new TierTopicMaterializationToolConfig(props), offsetMap);
        utils.run();

        System.out.println("**** Calling validator. \n");
        // Validate the state files.
        Iterator<TopicIdPartition> it = utils.stateMap.keySet().iterator();
        while (it.hasNext()) {
            try {
                TopicIdPartition eid = it.next();
                Path eFile = utils.getTierStateFile(eid);
                Path aFile = stateMap.get(eid).snapshot;
                if (validateStates(eFile, aFile, eid.topicPartition(),
                                    utils.getStartOffset(eid.topicPartition()))) {
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
     * @param tierStateFolder path of snapshot folder if populate is false else path of live data log folder.
     * @param populate if true than copy live tierState file of user partitions to snapshot folder.
     * @throws IOException
     * In case of unhandled exception we will let the exception terminate the application.
     */
    private void snapshotStateFiles(File tierStateFolder, boolean populate) throws IOException {
        if (!tierStateFolder.isDirectory()) {
            System.err.println(tierStateFolder + " is not metadata states directory");
            System.exit(1);
        }

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

                File snapShotFile = getSnapshotFilePath(topicPartition).toFile();
                if (populate) {
                    if (!snapShotFile.exists())
                        snapShotFile.mkdir();
                    System.out.println("Found TierTopicPartition dir " + dir.toPath());
                }

                for (File file: dir.listFiles()) {
                    if (file.isFile() && Log.isTierStateFile(file)) {
                        Path ss = Paths.get(snapShotFile.toString(), file.getName());
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
            System.out.println("Can not find any metadata states file in " + tierStateFolder);
            System.exit(1);
        }
    }

    /**
     * maybeActiveState helps in identifying the metadata object 'metadata' to be currently in used
     * for fetching the records. The 'startOffset' represents the log's starting offset.
     * There can be scenario when 'startOffset' may not be available -say verifying from recorded states,
     * the method will find the first metadata state which represents an active mapping.
     */
    private boolean mayBeActiveObject(long startOffset, TierObjectMetadata metadata) {
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
        FileChannel achannel = FileChannel.open(actual, StandardOpenOption.READ);
        FileChannel echannel = FileChannel.open(expected, StandardOpenOption.READ);
        FileTierPartitionIterator eiterator = FileTierPartitionState.iterator(id, echannel).get();
        FileTierPartitionIterator aiterator = FileTierPartitionState.iterator(id, achannel).get();

        if (!comparesStates(actual, expected) || !isValidStates(eiterator, aiterator, startOffset)) {
            System.out.println("Metadata inconsistencies(" + id + ") " + actual + "Vs " + expected);
            return false;
        }
        return true;
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

    public boolean isValidStates(Iterator<TierObjectMetadata> eIterator, Iterator<TierObjectMetadata> aIterator,
                        long firstValidOffset) {
        long prevEndOffset = -1;

        while (eIterator.hasNext()) {
            if (!aIterator.hasNext()) {
                System.err.println("Metadata inconsistency(more states).");
                return false;
            }

            TierObjectMetadata expectedObject = eIterator.next();
            TierObjectMetadata actualObject = aIterator.next();
            boolean active = mayBeActiveObject(firstValidOffset, actualObject);

            if (actualObject.equals(expectedObject)) {
                long start = Math.max(expectedObject.baseOffset(), prevEndOffset + 1);
                if ((start - prevEndOffset != 1) || (actualObject.endOffset() <= prevEndOffset)) {
                    if (active) {
                        if (actualObject.state().equals(State.SEGMENT_FENCED))
                            continue;
                        System.err.println("Metadata offset inconsistency "
                            + actualObject.baseOffset() + " : " + actualObject.endOffset());
                        return false;
                    } else {
                        // We found hole in offset range of states mappings. This is possible
                        // due to various scenario's like - retention before archival, first tiered
                        // offset etc. None of this is possible for active offsets (offset which is
                        // active for reading). Since this is inactive offset range, we will ignore.
                    }
                }
            } else {
                System.err.println("Metadata states inconsistency.");
                return false;
            }

            prevEndOffset = expectedObject.endOffset();
        }

        if (eIterator.hasNext() || aIterator.hasNext()) {
            System.err.println("Metadata inconsistency(more states).");
            return false;
        }
        return true;
    }

    private class TierMetadataValidatorRecord {
        public Path snapshot;
        public TopicIdPartition id;
        public Long maxOffset;

        public TierMetadataValidatorRecord(Path stateFile, TopicPartition topicPartition) throws IOException {
            FileChannel fileChannel = FileChannel.open(stateFile, StandardOpenOption.READ);
            Optional<Header>  headerOpt = FileTierPartitionState.readHeader(fileChannel);
            if (!headerOpt.isPresent())
                return;
            Header header = headerOpt.get();
            this.snapshot = stateFile;
            this.id = new TopicIdPartition(topicPartition.topic(), header.topicId(), topicPartition.partition());
            this.maxOffset = header.localMaterializedOffset();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Atleast " + TierTopicMaterializationToolConfig.METADATA_STATES_DIR + " needs to be set.");
            System.exit(1);
        }
        try {
            TierMetadataValidator validator = new TierMetadataValidator(args);
            validator.run();
        } catch (Exception ae) {
            System.out.println("Exception: " +  ae.getMessage());
            ae.printStackTrace();
            System.exit(1);
        }
    }
}

