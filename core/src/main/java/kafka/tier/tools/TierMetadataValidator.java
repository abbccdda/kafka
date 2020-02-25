/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import kafka.log.Log;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
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

        OptionSet options = parser.parse(args);

        if (!options.hasArgument(metaStatesDirSpec)) {
            System.err.println("Required arg " + TierTopicMaterializationToolConfig.METADATA_STATES_DIR);
            System.exit(1);
        }

        props.put(TierTopicMaterializationToolConfig.METADATA_STATES_DIR, options.valueOf(metaStatesDirSpec));
        props.put(TierTopicMaterializationToolConfig.WORKING_DIR, options.valueOf(workingDirSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_METADATA, "true");
        props.put(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG, options.valueOf(bootStrapServerSpec));
        props.put(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION, options.valueOf(tierSytateTopicPartitionSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_EVENTS, options.valueOf(dumpEventsSpec));

        System.out.println("Starting Validation with following args " + props + " " + props.get(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION));
    }

    private void createWorkDir(String dir) {
        File file = new File(dir);

        if (!file.exists())
            file.mkdirs();

        if (!file.isDirectory() || file.listFiles().length != 0) {
            System.err.println("materialization-path needs to be directory and should be empty");
            System.exit(1);
        }

        File snapshotDir = new File(getSnapshotDir(dir));
        if (snapshotDir.exists() && (!snapshotDir.isDirectory() || snapshotDir.listFiles().length != 0)) {
            System.err.println("snapshot path " + snapshotDir.getAbsolutePath() + " exists but is not directory or " +
                    "is not empty.");
            System.exit(1);
        } else {
            snapshotDir.mkdir();
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
        snapshotStateFiles(this.metadataStatesDir);

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
        // Compare the state files.
        Iterator<TopicIdPartition> it = utils.stateMap.keySet().iterator();
        while (it.hasNext()) {
            try {
                TopicIdPartition eid = it.next();
                Path eFile = utils.getTierStateFile(eid);
                Path aFile = stateMap.get(eid).snapshot;
                if (compareStates(eFile, aFile, eid.topicPartition())) {
                    System.out.println("Metadata states is consistent " + eFile + " Vs " + aFile);
                }
            } catch (Exception ex) {
                System.out.println("Ignoring comparison for non local.");
            }
        }
    }

    private void snapshotStateFiles(String metadataStatesDir) throws IOException {
        File mdir = new File(metadataStatesDir);
        if (!mdir.isDirectory()) {
            System.err.println(metadataStatesDir + " is not metadata states directory");
            System.exit(1);
        }

        for (File dir: mdir.listFiles()) {
            if (dir.isDirectory()) {
                try {
                    final TopicPartition topicPartition = Log.parseTopicPartitionName(dir);
                    File snapShotFile = getSnapshotFilePath(topicPartition).toFile();
                    if (!snapShotFile.exists())
                        snapShotFile.mkdir();
                    System.out.println("Found TierTopicPartition dir " + dir.toPath());

                    for (File file: dir.listFiles()) {
                        if (file.isFile() && Log.isTierStateFile(file)) {
                            System.out.println("Taking snapshot of partition states for " + topicPartition);
                            Path ss = Paths.get(snapShotFile.toString(), file.getName());
                            Files.copy(file.toPath(), ss);
                            System.out.println("Copied state files " + ss);
                            TierMetadataValidatorRecord record = new TierMetadataValidatorRecord(
                                    ss, topicPartition);
                            stateMap.put(record.id, record);
                        }
                    }
                } catch (KafkaException ex) {
                    // Ignore directories which are not Topic partition data directory. Rest of the exceptions will be
                    // still raised. There are cases when these data directories will
                }
            }
        }

        if (stateMap.isEmpty()) {
            System.out.println("Can not find any metadata states file in " + this.metadataStatesDir);
            System.exit(1);
        }
    }

    private Boolean compareStates(Path expected, Path actual, TopicPartition id) throws IOException {
        FileChannel echannel = FileChannel.open(expected, StandardOpenOption.READ);
        FileChannel achannel = FileChannel.open(expected, StandardOpenOption.READ);

        Header eheader = FileTierPartitionState.readHeader(echannel).get();
        Header aheader = FileTierPartitionState.readHeader(achannel).get();

        if (!eheader.equals(aheader)) {
            System.err.println("Metadata states(header) inconsistency " + expected + " Vs " + actual);
            return false;
        }

        byte[] f1 = Files.readAllBytes(expected);
        byte[] f2 = Files.readAllBytes(actual);
        if (!Arrays.equals(f1, f2)) {
            System.out.println("Metadata inconsistency(files do not match) " + expected + " Vs " + actual);
            return false;
        }

        Optional<FileTierPartitionIterator> eiteratorOpt = FileTierPartitionState.iterator(id, echannel);
        Optional<FileTierPartitionIterator> aiteratorOpt = FileTierPartitionState.iterator(id, achannel);

        long prevBaseOffset = -1, prevEndOffset = -1;
        boolean firstValid = true;

        while (eiteratorOpt.get().hasNext()) {
            if (!aiteratorOpt.get().hasNext()) {
                System.out.println("Metadata states inconsistency(more states in " + expected);
                return false;
            }

            TierObjectMetadata expectedObject = eiteratorOpt.get().next();
            TierObjectMetadata actualObject = aiteratorOpt.get().next();

            if (expectedObject.equals(actualObject)) {
                if (firstValid && ((actualObject.state().equals(TierObjectMetadata.State.SEGMENT_FENCED)) ||
                        (actualObject.state().equals(TierObjectMetadata.State.SEGMENT_DELETE_COMPLETE)))) {
                    // We will ignore the FENCED and DELETED segments, aim is to find the start offset.
                    continue;
                }

                long start = Math.max(expectedObject.baseOffset(), prevEndOffset + 1);
                if ((start - prevEndOffset != 1) || (expectedObject.endOffset() <= prevEndOffset)) {
                    // Start offset can be non zero and that needs to be handled.
                    if (firstValid)
                        continue;
                    System.err.println("Metadata offset inconsistency " + prevBaseOffset + " : " + prevEndOffset);
                    System.err.println("Expected : " + expected);
                    return false;
                }
            } else {
                System.err.println("Metadata states inconsistency " + expected + " Vs " + actual);
                return false;
            }
            prevBaseOffset = expectedObject.baseOffset();
            prevEndOffset = expectedObject.endOffset();
            firstValid = false;
        }

        if (eiteratorOpt.get().hasNext() || aiteratorOpt.get().hasNext()) {
            System.out.println("Metadata states inconsistency(more states in " + expected);
            return false;
        }

        return true;
    }

    class TierMetadataValidatorRecord {
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

