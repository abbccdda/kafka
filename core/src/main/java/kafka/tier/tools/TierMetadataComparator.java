/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import kafka.server.KafkaConfig;
import kafka.tier.TopicIdPartition;
import kafka.tier.fetcher.CancellationContext;
import kafka.tier.state.TierPartitionStatus;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStore.Backend;
import kafka.tier.store.TierObjectStoreConfig;
import kafka.tier.store.TierObjectStoreUtils;
import kafka.tier.tools.common.ComparatorInfo;
import kafka.tier.tools.common.FenceEventInfo;
import kafka.utils.CoreUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This tool will act as the downstream of the fencing tool in the recovery workflow
 * Once the fenced tier topic details are available as input, this tool will do the following:
 * 1. Rematerialize the affected tier topics up to the specified offsets
 * 2. Validate the tierstate files from the snapshots folders from all the bootstrap servers
 * 3. Also validate the affected tierstate files that were rematerialized in Step 1
 * 4. Finally recommend a tierstate file from the snapshot files that were copied based on largest endOffset
 * <p>
 * The way to run this tool is:
 * $ bin/kafka-run-class.sh kafka.tier.tools.TierMetadataComparator
 * --tier.config=/path/to/input.properties --input.json=/path/to/fencing.json --output.json=/path/to/output.json
 */
public class TierMetadataComparator implements AutoCloseable {
    private final Properties props;
    private final Path outputJsonFile;
    // This holds the parsed input which is emitted from the Fencing tool
    private final List<FenceEventInfo> fenceEvents;
    // A private handle to the materializationUtils class. Should only be used within the stateful methods
    private final TierTopicMaterializationUtils materializationUtils;
    // The idPartition -> tier topic offset at which fenced event was inserted
    // This map is generated from the parsed input list
    private final Map<TopicIdPartition, Long> fencedOffsetMap;
    // The cancellation context is used to signal to the validation process to terminate remote object store verification
    private final CancellationContext cancellationContext;
    // Private handle to represent which tier object store this comparator should connect to for remote object validation
    private final Optional<TierObjectStore> objStoreOpt;
    private final boolean offsetScanFlag;

    public static final List<String> REQUIRED_PROPERTIES = Arrays.asList(
            TierRecoveryConfig.BROKER_WORKDIR_LIST,
            TierRecoveryConfig.WORKING_DIR,
            TierRecoveryConfig.VALIDATE,
            TierRecoveryConfig.MATERIALIZE);

    public TierMetadataComparator(final Properties props, final List<FenceEventInfo> fenceEvents,
                                  Path outputJsonFile) {
        this.props = props;
        this.fenceEvents = fenceEvents;
        this.fencedOffsetMap = generateOffsetMapFromInput(fenceEvents);
        this.materializationUtils =
                new TierTopicMaterializationUtils(new TierTopicMaterializationToolConfig(TierRecoveryConfig.toMaterializerProperties(props)),
                new HashMap<>(this.fencedOffsetMap));
        this.cancellationContext = CancellationContext.newContext();
        this.outputJsonFile = outputJsonFile;
        this.objStoreOpt = getObjectStoreMaybe(this.props);
        this.offsetScanFlag = props.containsKey(TierRecoveryConfig.VALIDATE) &&
                Boolean.parseBoolean(props.getProperty(TierRecoveryConfig.VALIDATE));
    }

    static Optional<TierObjectStore> getObjectStoreMaybe(Properties props) {
        if (props.containsKey(TierRecoveryConfig.VALIDATE) &&
                Boolean.parseBoolean(props.getProperty(TierRecoveryConfig.VALIDATE))) {
            final Backend backend = Backend.valueOf(props.getProperty(KafkaConfig.TierBackendProp()));
            final TierObjectStoreConfig config = TierObjectStoreUtils.generateBackendConfig(backend, props);
            final TierObjectStore objStore = TierObjectStoreFactory.getObjectStoreInstance(backend, config);
            System.out.println("Initialized Backend: " + backend + " with objStore: " + objStore);
            return Optional.of(objStore);
        }
        System.out.println("Not initializing any backend, will avoid doing cloud object presence check!");
        return Optional.empty();
    }

    /**
     * The core logic of the Comparator tool can be broken down into the following 4 steps:
     * 1. Initially, the tool will initialize the materializationUtils with the offsetMap parsed from the input
     * This step is completed within the constructor
     * 2. Once the materializationUtils have been initialized, the tool would then materialize the affected tier topics
     * up to the offsets specified in the input
     * 3. Upon successful materialization of the affected tier topics onto the local disk, the tool would then
     * start the standalone validation of all the tierstate files produced by materialization, as well as,
     * from the files copied from the log directory of the brokers and provided through BROKER_WORKDIR_LIST
     * 4. Once, the tierstate files are validated, this tool would then generate the output in terms of both the
     * results of the validation tool run, as well as, it would recommend one of these tierstate files as the
     * recommended state, simply by comparing the maximum materialized offsets across a single topicIdPartition.
     * In order to maintain conformity with the functional interface of run, the tool would dump the JSON output into the
     * output Json file path provided
     */
    public void run() {
        System.out.println("Starting TierMetadataComparator with properties: " + props + " for partitions: " +
                Arrays.toString(fenceEvents.toArray()));

        Path baseMaterializationPath = Paths.get(props.getProperty(TierRecoveryConfig.WORKING_DIR));

        boolean materialize = Boolean.parseBoolean(props.getProperty(TierRecoveryConfig.MATERIALIZE));
        if (materialize) {
            try {
                materializationUtils.run();
            } catch (Exception e) {
                System.err.println("Failed to materialize states from tier state topic, " + e);
                throw new IllegalStateException("Failed to materialize states from tier state topic", e);
            }

            System.out.println("Materialized base states at: " + baseMaterializationPath + " for topicIdPartitions: " +
                    Arrays.toString(fencedOffsetMap.keySet().toArray()));
        }

        Map<String, Path> serverMap = getVerifiedTierFolderMap(props);
        serverMap.put(ComparatorInfo.REMATERIALIZED_REPLICA_ID, baseMaterializationPath);
        List<ComparatorInfo.ComparatorReplicaInfo> allReplicaInfo = getReplicas(fencedOffsetMap.keySet(), serverMap);

        allReplicaInfo.forEach(replicaInfo -> {
            validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                    materializationUtils::getStartOffset, objStoreOpt, offsetScanFlag,
                    TierPartitionStatus.ERROR);
        });
        System.out.println("Completed tier-state validation for info count: " + allReplicaInfo.size());
        generateChoiceAndWriteJsonOutput(fenceEvents, allReplicaInfo, outputJsonFile, fencedOffsetMap);
    }

    static void validateTierStateAndUpdateInfo(ComparatorInfo.ComparatorReplicaInfo info,
                                               CancellationContext cancellationContext,
                                               Function<TopicPartition, Long> startOffsetProducer,
                                               Optional<TierObjectStore> objStoreOpt,
                                               boolean offsetScanFlag,
                                               TierPartitionStatus requiredStatus) {
        try {
            final TierMetadataValidator.TierMetadataValidatorResult validatorResult = TierMetadataValidator.validateStandaloneTierStateFile(
                    info.tierStateFile(), info.topicIdPartition(), objStoreOpt, offsetScanFlag,
                    cancellationContext, startOffsetProducer);
            if (validatorResult.valid) {
                if (!validatorResult.headerOpt.isPresent())
                    throw new IllegalStateException("Valid state must have a header.");

                if (validatorResult.headerOpt.get().status() != requiredStatus)
                    throw new IllegalStateException("Validated TierPartitionState must be in "
                            + requiredStatus + " status.");
            }
            validatorResult.headerOpt.ifPresent(info::setHeader);
            info.setValidationSuccess(validatorResult.valid);
        } catch (Exception e) {
            // we will not fail the entire run if any of the validation fails, as this will void the comparison effort
            System.err.println("Couldn't validate replicaInfo: " + info + " due to exception: " + e);
            info.setException(e);
        }
    }

    /**
     * This method is responsible for choosing one replicaInfo per topicIdPartition by considering the maximum endOffset
     * amongst the various replicas present for that topicIdPartition
     * It also considers only those replicas whose validation status is successful and the endOffset <= fencedOffset
     *
     * @param allInfoList     the list of all replicas (updated with validation results) from which choices would be generated
     * @param fencedOffsetMap the map containing the fenced offset per topicIdPartition
     * @return the map containing a chosen replica per topicIdPartition
     */
    static Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> generateChoices(
            List<ComparatorInfo.ComparatorReplicaInfo> allInfoList, Map<TopicIdPartition, Long> fencedOffsetMap) {
        return allInfoList.stream()
                .filter(ComparatorInfo.ComparatorReplicaInfo::isValidationSuccess)
                .filter(info -> fencedOffsetMap.containsKey(info.topicIdPartition()) &&
                        fencedOffsetMap.get(info.topicIdPartition()) >= info.lastOffset())
                .collect(Collectors.groupingBy(ComparatorInfo.ComparatorReplicaInfo::topicIdPartition,
                        Collectors.maxBy(Comparator.comparingLong(ComparatorInfo.ComparatorReplicaInfo::lastOffset))));
    }

    /**
     * This method generates the choice of a replica per topicIdPartition using generateChoices() method
     * and then writes out the choices in the provided JSON output file
     *
     * @param fencedEvents    list of all the fenced events
     * @param infoList        list of all the replicas
     * @param outputJsonFile  path of the output for the comparator tool
     * @param fencedOffsetMap map with the fencedOffset per topicIdPartition
     */
    private static void generateChoiceAndWriteJsonOutput(List<FenceEventInfo> fencedEvents,
                                                         List<ComparatorInfo.ComparatorReplicaInfo> infoList,
                                                         Path outputJsonFile,
                                                         Map<TopicIdPartition, Long> fencedOffsetMap) {
        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> choiceMap =
                generateChoices(infoList, fencedOffsetMap);
        final Map<TopicIdPartition, List<ComparatorInfo.ComparatorReplicaInfo>> replicaMap =
                infoList.stream().collect(Collectors.groupingBy(ComparatorInfo.ComparatorReplicaInfo::topicIdPartition));
        final List<ComparatorInfo.ComparatorOutput> outputList = fencedEvents.stream().map(input -> {
            TopicIdPartition topicIdPartition = getTopicIdPartitionFromInput(input);
            Map<String, ComparatorInfo.ComparatorReplicaInfo> replicaInfoMap = replicaMap.get(topicIdPartition).stream()
                    .collect(Collectors.toMap(ComparatorInfo.ComparatorReplicaInfo::getReplica, info -> info));
            ComparatorInfo.ComparatorReplicaInfo choice = choiceMap.get(topicIdPartition).orElseGet(null);
            return new ComparatorInfo.ComparatorOutput(replicaInfoMap, choice, input);
        }).collect(Collectors.toList());
        try {
            ComparatorInfo.ComparatorOutput.writeJsonToFile(outputList, outputJsonFile);
            System.out.println("JSON Output written to: " + outputJsonFile);
        } catch (IOException e) {
            System.err.println("Error in writing out the Json output: " + outputJsonFile + "due to: " + e);
        }
    }

    static List<ComparatorInfo.ComparatorReplicaInfo> getReplicas(Set<TopicIdPartition> partitions,
                                                                  Map<String, Path> tierStateFolderMap) {
        List<ComparatorInfo.ComparatorReplicaInfo> allReplicaInfoList = new ArrayList<>();
        for (String replicaId : tierStateFolderMap.keySet()) {
            Path tierStateFolder = tierStateFolderMap.get(replicaId);
            System.out.println("Generating info for replica: " + replicaId + " with folder: " + tierStateFolder);
            try {
                List<ComparatorInfo.ComparatorReplicaInfo> infoList = TierMetadataValidator
                        .snapshotStateFiles(tierStateFolder.toFile(), false, tierStateFolder.toString())
                        .entrySet().stream()
                        .filter(entry -> partitions.contains(entry.getKey()))
                        .map(entry -> new ComparatorInfo.ComparatorReplicaInfo(replicaId, entry.getValue().snapshot, entry.getKey()))
                        .collect(Collectors.toList());
                if (infoList.size() != partitions.size()) {
                    throw new IllegalStateException("Couldn't collect all partitions for replica: " + replicaId);
                }
                allReplicaInfoList.addAll(infoList);
            } catch (Exception e) {
                System.err.println("Error in creating replicaInfo for replica: " + replicaId + " due to: " + e);
            }
        }
        System.out.println("Generated allReplicaInfoList count: " + allReplicaInfoList.size());
        return allReplicaInfoList;
    }

    static Map<TopicIdPartition, Long> generateOffsetMapFromInput(List<FenceEventInfo> fencedEvents) {
        List<Map.Entry<TopicIdPartition, Long>> fencedList = fencedEvents.stream().map(input -> {
            TopicIdPartition topicIdPartition = getTopicIdPartitionFromInput(input);
            return new AbstractMap.SimpleEntry<>(topicIdPartition, input.recordOffset);
        }).collect(Collectors.toList());
        Set<TopicIdPartition> topicIdPartitionSet = fencedList.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
        if (topicIdPartitionSet.size() != fencedList.size()) {
            throw new IllegalArgumentException("Duplicate topicIdPartitions as part of input fenced events: "
                    + Arrays.toString(fencedEvents.toArray()));
        }
        Map<TopicIdPartition, Long> offsetMap = fencedList.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        System.out.println("Generated offsetMap: " + Arrays.toString(offsetMap.keySet().toArray()));
        if (offsetMap.size() != fencedEvents.size()) {
            throw new IllegalArgumentException("Entire fencedEvents couldn't be converted to offsetMap!");
        }
        return offsetMap;
    }

    // exposed for tests
    static TopicIdPartition getTopicIdPartitionFromInput(FenceEventInfo input) {
        UUID id = CoreUtils.uuidFromBase64(input.topicIdBase64);
        return new TopicIdPartition(input.topic, id, input.partition);
    }

    private static List<FenceEventInfo> getComparatorInput(Path inputJsonFile) {
        if (Files.notExists(inputJsonFile) || !Files.isRegularFile(inputJsonFile)) {
            throw new IllegalArgumentException("Incorrect json file provided: " + inputJsonFile);
        }
        try {
            final List<FenceEventInfo> inputList = FenceEventInfo.jsonToList(inputJsonFile);
            System.out.println("Received JSON input: " + Arrays.toString(inputList.toArray()));
            return inputList;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Couldn't parse provided input JSON", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Incorrect JSON file provided: " + inputJsonFile, e);
        }
    }

    static Map<String, Path> getVerifiedTierFolderMap(Properties props) {
        String[] workingDirs = props.getProperty(TierRecoveryConfig.BROKER_WORKDIR_LIST).split(",");
        if (workingDirs.length == 0) {
            throw new IllegalArgumentException("Received empty: " + TierRecoveryConfig.BROKER_WORKDIR_LIST);
        }
        Map<String, Path> bootstrapServerMap = new HashMap<>();
        for (final String workingDir : workingDirs) {
            Path workdir = Paths.get(workingDir);
            if (Files.notExists(workdir) || !Files.isDirectory(workdir)) {
                throw new IllegalArgumentException("Incorrect workdir: " + workdir);
            }
            String replicaId = workdir.getFileName().toString();
            if (bootstrapServerMap.containsKey(replicaId)) {
                throw new IllegalArgumentException("Found duplicate replicaId " + replicaId + " in: "
                        + Arrays.toString(workingDirs));
            } else if (ComparatorInfo.REMATERIALIZED_REPLICA_ID.equals(replicaId)) {
                throw new IllegalArgumentException("replicaId can't be: " + ComparatorInfo.REMATERIALIZED_REPLICA_ID);
            }
            bootstrapServerMap.put(replicaId, workdir);
        }
        return bootstrapServerMap;
    }

    static ArgumentParser createComparatorParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser(TierMetadataComparator.class.getName())
                .defaultHelp(true)
                .description("Compares the tier-state files across different brokers");

        parser.addArgument(RecoveryUtils.makeArgument(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG))
                .dest(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG)
                .type(String.class)
                .required(true)
                .help(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_DOC);

        parser.addArgument(RecoveryUtils.makeArgument(RecoveryUtils.COMPARISON_TOOL_INPUT))
                .dest(RecoveryUtils.COMPARISON_TOOL_INPUT)
                .type(String.class)
                .required(true)
                .help(RecoveryUtils.COMPARISON_TOOL_INPUT_DOC);

        parser.addArgument(RecoveryUtils.makeArgument(RecoveryUtils.COMPARISON_TOOL_OUTPUT))
                .dest(RecoveryUtils.COMPARISON_TOOL_OUTPUT)
                .type(String.class)
                .required(true)
                .help(RecoveryUtils.COMPARISON_TOOL_OUTPUT_DOC);

        return parser;
    }

    static Properties fetchPropertiesFromArgs(String[] args, ArgumentParser parser) {
        try {
            Properties props = new Properties();
            Namespace res = parser.parseArgs(args);
            String toolsConfigFile = res.getString(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG);
            System.out.println("TierMetadataComparator received properties file: " + toolsConfigFile);
            props.putAll(Utils.loadProps(toolsConfigFile));
            for (String key : REQUIRED_PROPERTIES) {
                if (!props.containsKey(key)) {
                    throw new IllegalArgumentException("Properties doesn't contain key: " + key + ", allProps: " + props);
                }
            }
            System.out.println("fetchPropertiesFromArgs received props: " + props);
            return props;
        } catch (ArgumentParserException | IOException e) {
            throw new IllegalArgumentException("Couldn't create properties from provided file!", e);
        }
    }

    static Path createJsonPathFromArgs(String[] args, ArgumentParser parser, String argKey) {
        try {
            Namespace res = parser.parseArgs(args);
            Path inputFile = Paths.get(res.getString(argKey));
            System.out.println("TierMetadataComparator received " + argKey + " file: " + inputFile);
            return inputFile;
        } catch (ArgumentParserException e) {
            throw new IllegalArgumentException("Couldn't create " + argKey + " from provided file!", e);
        }
    }

    public static void main(String[] args) {
        System.out.println("Received cmdline args: " + Arrays.toString(args));
        final ArgumentParser cliParser = createComparatorParser();
        final Properties props = fetchPropertiesFromArgs(args, cliParser);
        final Path inputJsonFile = createJsonPathFromArgs(args, cliParser, RecoveryUtils.COMPARISON_TOOL_INPUT);
        final Path outputJsonFile = createJsonPathFromArgs(args, cliParser, RecoveryUtils.COMPARISON_TOOL_OUTPUT);
        final List<FenceEventInfo> partitions = getComparatorInput(inputJsonFile);
        try (final TierMetadataComparator comparator = new TierMetadataComparator(props, partitions, outputJsonFile)) {
            comparator.run();
        } catch (Exception e) {
            System.err.println("Received exception during comparator runtime");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * As part of the closeable contract, this method will cleanup all the open resources such as backend stores
     * as well as cancel any long-running existing verification tasks
     */
    @Override
    public void close() {
        cancellationContext.cancel();
        objStoreOpt.ifPresent(objectStore -> TierObjectStoreFactory.closeBackendInstance(objectStore.getBackend()));
    }
}
