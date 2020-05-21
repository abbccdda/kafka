package kafka.tier.tools;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.server.KafkaConfig;
import kafka.server.LogDirFailureChannel;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierSegmentUploadComplete;
import kafka.tier.domain.TierSegmentUploadInitiate;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.fetcher.CancellationContext;
import kafka.tier.state.FileTierPartitionState;
import kafka.tier.state.OffsetAndEpoch;
import kafka.tier.state.TierPartitionState;
import kafka.tier.store.TierObjectStore;
import kafka.tier.tools.common.ComparatorInfo;
import kafka.tier.tools.common.FenceEventInfo;
import kafka.utils.CoreUtils;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TierMetadataComparatorTest {
    private List<FenceEventInfo> inputList;
    private final TopicIdPartition idPartitionA = new TopicIdPartition("test-topic",
            CoreUtils.uuidFromBase64("0SoOrPUfRgaP7dExQdzWAg"), 0);
    private final TopicIdPartition idPartitionB = new TopicIdPartition("test-topic",
            CoreUtils.uuidFromBase64("0SoOrPUfRgaP7dExQdzWAg"), 42);
    private final Set<TopicIdPartition> idPartitionSet = new HashSet<TopicIdPartition>() {{
        add(idPartitionA);
        add(idPartitionB);
    }};
    private final Function<TopicPartition, Long> constantStartOffsetProducer = topic -> 0L;
    private final CancellationContext cancellationContext = CancellationContext.newContext();
    private final TierObjectStore.Backend backend = TierObjectStore.Backend.Mock;
    private Optional<TierObjectStore> objStoreOpt;

    private static final String TIER_STATE_FILE_NAME = "00000000000000000000.tierstate";
    private static final String REPLICA_ID_A = "hostA";
    private static final String REPLICA_ID_B = "hostB";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        revolvingOffset = new AtomicLong(-1L);
        inputList = new ArrayList<>();
        inputList.add(new FenceEventInfo("test-topic", "0SoOrPUfRgaP7dExQdzWAg",
                0, CoreUtils.uuidToBase64(UUID.randomUUID()), 201));
        inputList.add(new FenceEventInfo("test-topic", "0SoOrPUfRgaP7dExQdzWAg",
                42, CoreUtils.uuidToBase64(UUID.randomUUID()), 101));
        initializeObjectStore();
    }

    @After
    public void tearDown() {
        TierObjectStoreFactory.closeBackendInstance(backend);
    }

    private void initializeObjectStore() {
        Properties props = new Properties();
        props.setProperty(TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION, "true");
        props.setProperty("cluster-id", "mock-cluster");
        props.put(KafkaConfig.BrokerIdProp(), 1);
        props.setProperty(KafkaConfig.TierBackendProp(), backend.getName());
        objStoreOpt = TierMetadataComparator.getObjectStoreMaybe(props);
    }

    @Test
    public void testTierFolderMapMap() throws IOException {
        List<File> hostDirList = new ArrayList<File>() {{
            add(tempFolder.newFolder("hostA"));
            add(tempFolder.newFolder("hostB"));
        }};
        Properties props = new Properties();
        props.setProperty(TierMetadataComparator.BROKER_WORKDIR_LIST,
                hostDirList.stream().map(File::getAbsolutePath).collect(Collectors.joining(",")));
        final Map<String, Path> hostPathMap = TierMetadataComparator.getVerifiedTierFolderMap(props);
        assertEquals("Unexpected hostPathMap length!", 2, hostPathMap.size());
        assertEquals("Incorrect hostA Path", hostDirList.get(0).toPath(), hostPathMap.get(REPLICA_ID_A));
        assertEquals("Incorrect hostA Path", hostDirList.get(1).toPath(), hostPathMap.get(REPLICA_ID_B));
    }

    @Test
    public void testTierFolderMapThrowsOnNonExistentFolder() throws IOException {
        Properties props = new Properties();
        props.setProperty(TierMetadataComparator.BROKER_WORKDIR_LIST, "/path/to/hostA");
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TierMetadataComparator.getVerifiedTierFolderMap(props));
        assertEquals("Incorrect exception message", "Incorrect workdir: /path/to/hostA", exception.getMessage());
    }

    @Test
    public void testTierFolderMapThrowsOnRematerializedKey() throws IOException {
        final Path workdir = tempFolder.newFolder(ComparatorInfo.REMATERIALIZED_REPLICA_ID).toPath();
        Properties props = new Properties();
        props.setProperty(TierMetadataComparator.BROKER_WORKDIR_LIST, workdir.toString());
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TierMetadataComparator.getVerifiedTierFolderMap(props));
        assertEquals("Incorrect exception message",
                "replicaId can't be: " + ComparatorInfo.REMATERIALIZED_REPLICA_ID, exception.getMessage());
    }

    @Test
    public void testTierFolderMapThrowsOnDuplicateKey() throws IOException {
        final Path workdir = tempFolder.newFolder(REPLICA_ID_A).toPath();
        Properties props = new Properties();
        props.setProperty(TierMetadataComparator.BROKER_WORKDIR_LIST, workdir.toString() + "," + workdir.toString());
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> TierMetadataComparator.getVerifiedTierFolderMap(props));
        assertTrue("Incorrect exception message", exception.getMessage().contains("Found duplicate replicaId " + REPLICA_ID_A));
    }

    @Test
    public void testComparatorInputGeneration() throws IOException {
        File tempInputJsonFile = tempFolder.newFile("input.json");
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(tempInputJsonFile, inputList);
        final List<FenceEventInfo> receivedInputList = FenceEventInfo.jsonToList(tempInputJsonFile.toPath());
        assertEquals("Unexpected receivedInputList length!", inputList.size(), receivedInputList.size());
        assertEquals("Incorrect idPartitionA", inputList.get(0).toJson(), receivedInputList.get(0).toJson());
        assertEquals("Incorrect idPartitionB", inputList.get(1).toJson(), receivedInputList.get(1).toJson());
    }

    @Test
    public void testTopicIdPartitionFromInput() {
        assertEquals("Incorrect idPartitionA", idPartitionA, TierMetadataComparator.getTopicIdPartitionFromInput(inputList.get(0)));
        assertEquals("Incorrect idPartitionB", idPartitionB, TierMetadataComparator.getTopicIdPartitionFromInput(inputList.get(1)));
    }

    @Test
    public void testOffsetMapGeneration() {
        final Map<TopicIdPartition, Long> offsetMap = TierMetadataComparator.generateOffsetMapFromInput(inputList);
        assertEquals("Unexpected offsetMap length!", 2, offsetMap.size());
        assertEquals("Incorrect idPartitionA offset", 201L, (long) offsetMap.get(idPartitionA));
        assertEquals("Incorrect idPartitionA offset", 101L, (long) offsetMap.get(idPartitionB));
    }

    @Test
    public void testReplicaGenerationFromInput() throws IOException {
        Map<String, Path> tierStateFolderMap = new HashMap<String, Path>() {{
            put(REPLICA_ID_A, tempFolder.newFolder(REPLICA_ID_A).toPath());
            put(REPLICA_ID_B, tempFolder.newFolder(REPLICA_ID_B).toPath());
        }};

        final List<ComparatorInfo.ComparatorReplicaInfo> replicaList = generateReplicaInfo(tierStateFolderMap);
        final List<String> replicaJsonList = replicaList.stream()
                .map(ComparatorInfo.ComparatorReplicaInfo::toJson)
                .collect(Collectors.toList());

        // the constant 2 signifies two distinct topicIdPartitions: idPartitionA, idPartitionB
        assertEquals("Incorrect replicaList size", 2 * inputList.size(), replicaList.size());

        // verifying details for replica: hostA:9092
        Path hostAidPartitionAFile = TierMetadataValidator
                .getSnapshotFilePath(idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_A).toString())
                .resolve(TIER_STATE_FILE_NAME);
        Path hostAidPartitionBFile = TierMetadataValidator
                .getSnapshotFilePath(idPartitionB.topicPartition(), tierStateFolderMap.get(REPLICA_ID_A).toString())
                .resolve(TIER_STATE_FILE_NAME);

        assertTrue("hostA:9092 didn't contain replica for: " + idPartitionA,
                replicaJsonList.contains(new ComparatorInfo.ComparatorReplicaInfo(REPLICA_ID_A, hostAidPartitionAFile, idPartitionA).toJson()));
        assertTrue("hostA:9092 didn't contain replica for: " + idPartitionB,
                replicaJsonList.contains(new ComparatorInfo.ComparatorReplicaInfo(REPLICA_ID_A, hostAidPartitionBFile, idPartitionB).toJson()));

        // verifying details for replica: hostB:9092
        Path hostBidPartitionAFile = TierMetadataValidator
                .getSnapshotFilePath(idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_B).toString())
                .resolve(TIER_STATE_FILE_NAME);
        Path hostBidPartitionBFile = TierMetadataValidator
                .getSnapshotFilePath(idPartitionB.topicPartition(), tierStateFolderMap.get(REPLICA_ID_B).toString())
                .resolve(TIER_STATE_FILE_NAME);

        assertTrue("hostB:9092 didn't contain replica for: " + idPartitionA,
                replicaJsonList.contains(new ComparatorInfo.ComparatorReplicaInfo(REPLICA_ID_B, hostBidPartitionAFile, idPartitionA).toJson()));
        assertTrue("hostB:9092 didn't contain replica for: " + idPartitionB,
                replicaJsonList.contains(new ComparatorInfo.ComparatorReplicaInfo(REPLICA_ID_B, hostBidPartitionBFile, idPartitionB).toJson()));

    }

    @Test
    public void testSimpleTierStateFileValidation() throws IOException {
        Path workdir = tempFolder.newFolder(REPLICA_ID_A).toPath();
        Path idPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), workdir.toString()));
        generateTierStateFile(idPartitionA, generateOffsetList(2), idPartitionADir);
        final TierMetadataValidator.TierMetadataValidatorResult result = TierMetadataValidator.validateStandaloneTierStateFile(idPartitionADir.resolve(TIER_STATE_FILE_NAME),
                idPartitionA, Optional.empty(), false, cancellationContext, constantStartOffsetProducer);
        assertTrue(result.headerOpt.isPresent());
        assertTrue(result.valid);
        assertEquals(idPartitionA.topicId(), result.headerOpt.get().topicId());
        // we expect the final offset to be at 4, because we have replayed 5 events
        assertEquals(4L, result.headerOpt.get().localMaterializedOffsetAndEpoch().offset());
    }

    @Test
    public void testSimpleOffsetInconsistencies() throws IOException {
        final ArrayList<AbstractMap.SimpleImmutableEntry<Long, Long>> inconsistentOffsetList =
                new ArrayList<AbstractMap.SimpleImmutableEntry<Long, Long>>() {{
                    add(new AbstractMap.SimpleImmutableEntry<Long, Long>(0L, 100L));
                    // note there is a hole in the segment offsets here
                    add(new AbstractMap.SimpleImmutableEntry<Long, Long>(102L, 200L));
                }};
        Path workdir = tempFolder.newFolder(REPLICA_ID_A).toPath();
        Path idPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), workdir.toString()));
        generateTierStateFile(idPartitionA, inconsistentOffsetList, idPartitionADir);
        final TierMetadataValidator.TierMetadataValidatorResult result = TierMetadataValidator.validateStandaloneTierStateFile(idPartitionADir.resolve(TIER_STATE_FILE_NAME),
                idPartitionA, objStoreOpt, false, cancellationContext, constantStartOffsetProducer);
        assertTrue(result.headerOpt.isPresent());
        assertFalse(result.valid);
    }

    @Test
    public void testInfoIsUpdatedOnValidation() throws IOException {
        Map<String, Path> tierStateFolderMap = new HashMap<String, Path>() {{
            put(REPLICA_ID_A, tempFolder.newFolder(REPLICA_ID_A).toPath());
        }};

        final List<ComparatorInfo.ComparatorReplicaInfo> replicaList = generateReplicaInfo(tierStateFolderMap);

        // the size should be 2 because each host will have two topicIdPartitions
        assertEquals(2, replicaList.size());
        replicaList.forEach(replicaInfo -> {
            TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                    constantStartOffsetProducer, objStoreOpt, false);
            if (idPartitionA.equals(replicaInfo.topicIdPartition())) {
                assertEquals(idPartitionA.topicId(), replicaInfo.header.topicId());
                assertEquals(4L, replicaInfo.header.localMaterializedOffsetAndEpoch().offset());
            } else if (idPartitionB.equals(replicaInfo.topicIdPartition())) {
                assertEquals(idPartitionB.topicId(), replicaInfo.header.topicId());
                assertEquals(2L, replicaInfo.header.localMaterializedOffsetAndEpoch().offset());
            }
            assertEquals(REPLICA_ID_A, replicaInfo.getReplica());
        });
    }

    @Test
    public void testValidationFailsOnOffsetScanEnable() throws IOException {
        Map<String, Path> tierStateFolderMap = new HashMap<String, Path>() {{
            put(REPLICA_ID_A, tempFolder.newFolder(REPLICA_ID_A).toPath());
        }};

        final List<ComparatorInfo.ComparatorReplicaInfo> replicaList = generateReplicaInfo(tierStateFolderMap);

        // the size should be 2 because each host will have two topicIdPartitions
        assertEquals(2, replicaList.size());
        replicaList.forEach(replicaInfo -> {
            TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                    constantStartOffsetProducer, objStoreOpt, true);
            assertFalse(replicaInfo.isValidationSuccess());
            assertNotNull(replicaInfo.header);
            assertEquals(REPLICA_ID_A, replicaInfo.getReplica());
        });
    }

    @Test
    public void testChoiceRespectsHigherEndOffset() throws IOException {
        Map<String, Path> tierStateFolderMap = new HashMap<String, Path>() {{
            put(REPLICA_ID_A, tempFolder.newFolder(REPLICA_ID_A).toPath());
            put(REPLICA_ID_B, tempFolder.newFolder(REPLICA_ID_B).toPath());
        }};

        Map<TopicIdPartition, Long> offsetMap = inputList.stream().collect(Collectors.toMap(
                TierMetadataComparator::getTopicIdPartitionFromInput, FenceEventInfo::recordOffset));

        // Initially, we are going to configure the hosts accordingly (note offsets start at 0)
        // hostA:9092 { idPartitionA: 5 events, idPartitionB: 3 events } => expected lastOffset for choice: 4L
        // hostB:9092 { idPartitionA: 3 events, idPartitionB: 7 events } => expected lastOffset for choice: 6L
        Path hostAidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_A).toString()));
        Path hostAidPartitionBDir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionB.topicPartition(), tierStateFolderMap.get(REPLICA_ID_A).toString()));

        Path hostBidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_B).toString()));
        Path hostBidPartitionBDir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionB.topicPartition(), tierStateFolderMap.get(REPLICA_ID_B).toString()));

        generateTierStateFile(idPartitionA, generateOffsetList(2), hostAidPartitionADir);
        resetOffset();
        // we are keeping a reference to the file for appending another tier topic event
        final FileTierPartitionState hostAidPartitionBFile = generateTierStateFile(idPartitionB, generateOffsetList(1),
                hostAidPartitionBDir);
        resetOffset();
        generateTierStateFile(idPartitionA, generateOffsetList(1), hostBidPartitionADir);
        resetOffset();
        generateTierStateFile(idPartitionB, generateOffsetList(3), hostBidPartitionBDir);

        final List<ComparatorInfo.ComparatorReplicaInfo> validatedReplicaList = TierMetadataComparator
                .getReplicas(idPartitionSet, tierStateFolderMap)
                .stream()
                .peek(replicaInfo ->
                        TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                                constantStartOffsetProducer, objStoreOpt, false)
                ).collect(Collectors.toList());

        // the size should be 4 because each host will have two topicIdPartitions
        assertEquals(4, validatedReplicaList.size());

        // Initially, hostA, having the higher number of events for idPartitionA should be the choice for idPartitionA
        // Similarly, hostB, having the higher number of events for idPartitionB should be the choice for idPartitionB
        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> choiceMap =
                TierMetadataComparator.generateChoices(validatedReplicaList, offsetMap);

        assertEquals(2, choiceMap.size());
        assertTrue(choiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_A, choiceMap.get(idPartitionA).get().getReplica());
        assertTrue(choiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(4L, choiceMap.get(idPartitionA).get().lastOffset());

        assertTrue(choiceMap.get(idPartitionB).isPresent());
        assertEquals(REPLICA_ID_B, choiceMap.get(idPartitionB).get().getReplica());
        assertTrue(choiceMap.get(idPartitionB).get().isValidationSuccess());
        assertEquals(6L, choiceMap.get(idPartitionB).get().lastOffset());

        // Now we are going to add another event on hostA at offset 42 for idPartitionB
        assertEquals(TierPartitionState.AppendResult.ACCEPTED, hostAidPartitionBFile.append(
                new TierTopicInitLeader(idPartitionB, 1, UUID.randomUUID(), 0),
                new OffsetAndEpoch(42L, Optional.of(1))));
        hostAidPartitionBFile.flush();

        // Now when we reevaluate the choices, hostA should be the choice for idPartitionB
        final List<ComparatorInfo.ComparatorReplicaInfo> updatedReplicaList = TierMetadataComparator
                .getReplicas(idPartitionSet, tierStateFolderMap)
                .stream()
                .peek(replicaInfo ->
                        TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                                constantStartOffsetProducer, objStoreOpt, false)
                ).collect(Collectors.toList());

        // the size should be 4 because each host will have two topicIdPartitions
        assertEquals(4, updatedReplicaList.size());
        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> updatedChoiceMap =
                TierMetadataComparator.generateChoices(updatedReplicaList, offsetMap);

        // idPartitionA choice remains completely unchanged
        assertEquals(2, updatedChoiceMap.size());
        assertTrue(updatedChoiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_A, updatedChoiceMap.get(idPartitionA).get().getReplica());
        assertTrue(updatedChoiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(4L, updatedChoiceMap.get(idPartitionA).get().lastOffset());

        // However, idPartitionB is updated with hostA due to the latest initLeader event at offset 42
        assertTrue(updatedChoiceMap.get(idPartitionB).isPresent());
        assertEquals(REPLICA_ID_A, updatedChoiceMap.get(idPartitionB).get().getReplica());
        assertTrue(updatedChoiceMap.get(idPartitionB).get().isValidationSuccess());
        assertEquals(42L, updatedChoiceMap.get(idPartitionB).get().lastOffset());
    }

    @Test
    public void testChoiceRespectsValidatorResult() throws IOException {
        Map<String, Path> tierStateFolderMap = new HashMap<String, Path>() {{
            put(REPLICA_ID_A, tempFolder.newFolder(REPLICA_ID_A).toPath());
            put(REPLICA_ID_B, tempFolder.newFolder(REPLICA_ID_B).toPath());
        }};

        Map<TopicIdPartition, Long> offsetMap = inputList.stream().collect(Collectors.toMap(
                TierMetadataComparator::getTopicIdPartitionFromInput, FenceEventInfo::recordOffset));

        // Initially, we are going to configure the host accordingly (note offsets start at 0)
        // hostA:9092 { idPartitionA: 5 events } => expected lastOffset for choice: 4L
        // hostB:9092 { idPartitionA: 3 events }
        Path hostAidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_A).toString()));
        Path hostBidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_B).toString()));

        generateTierStateFile(idPartitionA, generateOffsetList(2), hostAidPartitionADir);
        resetOffset();
        generateTierStateFile(idPartitionA, generateOffsetList(1), hostBidPartitionADir);

        final Set<TopicIdPartition> singletonSet = Collections.singleton(idPartitionA);

        final List<ComparatorInfo.ComparatorReplicaInfo> validatedReplicaList = TierMetadataComparator
                .getReplicas(singletonSet, tierStateFolderMap)
                .stream()
                .peek(replicaInfo ->
                        TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                                constantStartOffsetProducer, objStoreOpt, false)
                ).collect(Collectors.toList());

        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> choiceMap =
                TierMetadataComparator.generateChoices(validatedReplicaList, offsetMap);

        assertEquals(2, validatedReplicaList.size());
        assertTrue(choiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_A, choiceMap.get(idPartitionA).get().getReplica());
        assertTrue(choiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(4L, choiceMap.get(idPartitionA).get().lastOffset());

        // next we explicitly update the validation flag for hostA to false, so that its excluded from the choice
        final List<ComparatorInfo.ComparatorReplicaInfo> failingReplicaList =
                validatedReplicaList.stream()
                        .peek(info -> {
                            if (REPLICA_ID_A.equals(info.getReplica())) {
                                info.setValidationSuccess(false);
                            }
                        }).collect(Collectors.toList());

        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> updatedChoiceMap =
                TierMetadataComparator.generateChoices(failingReplicaList, offsetMap);

        // Upon reevaluating, we can see that choice has been updated to hostB
        assertEquals(2, failingReplicaList.size());
        assertTrue(updatedChoiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_B, updatedChoiceMap.get(idPartitionA).get().getReplica());
        assertTrue(updatedChoiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(2L, updatedChoiceMap.get(idPartitionA).get().lastOffset());
    }

    @Test
    public void testChoiceRespectsMetadataValidatorResult() throws IOException {
        Map<String, Path> tierStateFolderMap = new HashMap<String, Path>() {{
            put(REPLICA_ID_A, tempFolder.newFolder(REPLICA_ID_A).toPath());
            put(REPLICA_ID_B, tempFolder.newFolder(REPLICA_ID_B).toPath());
        }};

        Map<TopicIdPartition, Long> offsetMap = inputList.stream().collect(Collectors.toMap(
                TierMetadataComparator::getTopicIdPartitionFromInput, FenceEventInfo::recordOffset));

        // Initially, we are going to configure the host accordingly (note offsets start at 0)
        // hostA:9092 { idPartitionA: 5 events } => expected lastOffset for choice: 4L
        // hostB:9092 { idPartitionA: 3 events }
        Path hostAidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_A).toString()));
        Path hostBidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_B).toString()));

        // we are retaining a handle to the offsetList to replay the last element
        final List<AbstractMap.SimpleImmutableEntry<Long, Long>> hostAOffsetList = generateOffsetList(3);
        final FileTierPartitionState hostATierStateFile = generateTierStateFile(idPartitionA,
                hostAOffsetList.subList(0, hostAOffsetList.size() - 1),
                hostAidPartitionADir);
        resetOffset();
        generateTierStateFile(idPartitionA, generateOffsetList(1), hostBidPartitionADir);

        final Set<TopicIdPartition> singletonSet = Collections.singleton(idPartitionA);

        final List<ComparatorInfo.ComparatorReplicaInfo> validatedReplicaList = TierMetadataComparator
                .getReplicas(singletonSet, tierStateFolderMap)
                .stream()
                .peek(replicaInfo ->
                        TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                                constantStartOffsetProducer, objStoreOpt, false)
                ).collect(Collectors.toList());

        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> choiceMap =
                TierMetadataComparator.generateChoices(validatedReplicaList, offsetMap);

        assertEquals(2, validatedReplicaList.size());
        assertTrue(choiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_A, choiceMap.get(idPartitionA).get().getReplica());
        assertTrue(choiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(4L, choiceMap.get(idPartitionA).get().lastOffset());

        // Next up, we replay both the uploadInit and uploadComplete event without actually writing the segment file
        revolvingOffset.set(hostATierStateFile.endOffset());
        final AbstractMap.SimpleImmutableEntry<Long, Long> entry = hostAOffsetList.get(hostAOffsetList.size() - 1);
        final TierSegmentUploadInitiate uploadInit =
                new TierSegmentUploadInitiate(idPartitionA, 0, UUID.randomUUID(), entry.getKey(), entry.getValue(),
                        1, 100, false, false, false, getNextOffset());
        assertEquals(TierPartitionState.AppendResult.ACCEPTED, hostATierStateFile.append(uploadInit, uploadInit.stateOffsetAndEpoch()));
        // we aren't writing the actual segment files here
        assertEquals(TierPartitionState.AppendResult.ACCEPTED,
                hostATierStateFile.append(new TierSegmentUploadComplete(uploadInit), getNextOffset()));
        // we are flushing the tierstate with nonexistent segment upload complete message
        hostATierStateFile.flush();

        final List<ComparatorInfo.ComparatorReplicaInfo> failingReplicaList = TierMetadataComparator
                .getReplicas(singletonSet, tierStateFolderMap)
                .stream()
                .peek(replicaInfo ->
                        TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                                constantStartOffsetProducer, objStoreOpt, false)
                ).collect(Collectors.toList());

        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> updatedChoiceMap =
                TierMetadataComparator.generateChoices(failingReplicaList, offsetMap);

        // Upon reevaluating, we can see that choice has been updated to hostB
        assertEquals(2, failingReplicaList.size());
        assertTrue(updatedChoiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_B, updatedChoiceMap.get(idPartitionA).get().getReplica());
        assertTrue(updatedChoiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(2L, updatedChoiceMap.get(idPartitionA).get().lastOffset());
    }

    @Test
    public void testChoiceRespectsFencedOffsetInput() throws IOException {
        Map<String, Path> tierStateFolderMap = new HashMap<String, Path>() {{
            put(REPLICA_ID_A, tempFolder.newFolder(REPLICA_ID_A).toPath());
            put(REPLICA_ID_B, tempFolder.newFolder(REPLICA_ID_B).toPath());
        }};

        Map<TopicIdPartition, Long> offsetMap = inputList.stream().collect(Collectors.toMap(
                TierMetadataComparator::getTopicIdPartitionFromInput, FenceEventInfo::recordOffset));

        // Initially, we are going to configure the host accordingly (note offsets start at 0)
        // hostA:9092 { idPartitionA: 5 events } => expected lastOffset for choice: 4L
        // hostB:9092 { idPartitionA: 3 events }
        Path hostAidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_A).toString()));
        Path hostBidPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                idPartitionA.topicPartition(), tierStateFolderMap.get(REPLICA_ID_B).toString()));

        FileTierPartitionState hostAFile = generateTierStateFile(idPartitionA, generateOffsetList(2), hostAidPartitionADir);
        resetOffset();
        generateTierStateFile(idPartitionA, generateOffsetList(1), hostBidPartitionADir);

        final Set<TopicIdPartition> singletonSet = Collections.singleton(idPartitionA);

        final List<ComparatorInfo.ComparatorReplicaInfo> validatedReplicaList = TierMetadataComparator
                .getReplicas(singletonSet, tierStateFolderMap)
                .stream()
                .peek(replicaInfo ->
                        TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                                constantStartOffsetProducer, objStoreOpt, false)
                ).collect(Collectors.toList());

        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> choiceMap =
                TierMetadataComparator.generateChoices(validatedReplicaList, offsetMap);

        assertEquals(2, validatedReplicaList.size());
        assertTrue(choiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_A, choiceMap.get(idPartitionA).get().getReplica());
        assertTrue(choiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(4L, choiceMap.get(idPartitionA).get().lastOffset());

        // next we put in an initLeader event at an offset higher than our fenced offset
        assertEquals(TierPartitionState.AppendResult.ACCEPTED,
                hostAFile.append(new TierTopicInitLeader(idPartitionA, 1, UUID.randomUUID(), 0),
                        new OffsetAndEpoch(offsetMap.get(idPartitionA) + 1, Optional.of(1))));
        hostAFile.flush();

        final List<ComparatorInfo.ComparatorReplicaInfo> updatedReplicaList = TierMetadataComparator
                .getReplicas(singletonSet, tierStateFolderMap)
                .stream()
                .peek(replicaInfo ->
                        TierMetadataComparator.validateTierStateAndUpdateInfo(replicaInfo, cancellationContext,
                                constantStartOffsetProducer, objStoreOpt, false)
                ).collect(Collectors.toList());

        final Map<TopicIdPartition, Optional<ComparatorInfo.ComparatorReplicaInfo>> updatedChoiceMap =
                TierMetadataComparator.generateChoices(updatedReplicaList, offsetMap);

        // Upon reevaluating, we can see that choice has been updated to hostB
        assertEquals(2, updatedReplicaList.size());
        assertTrue(updatedChoiceMap.get(idPartitionA).isPresent());
        assertEquals(REPLICA_ID_B, updatedChoiceMap.get(idPartitionA).get().getReplica());
        assertTrue(updatedChoiceMap.get(idPartitionA).get().isValidationSuccess());
        assertEquals(2L, updatedChoiceMap.get(idPartitionA).get().lastOffset());
    }

    /**
     * This method return offset pairs to be used to create segment baseOffset and endOffsets
     *
     * @param size no of such pairs to be created
     * @return list of [<baseOffset1, endOffset1>, <baseOffset2, endOffset2>...]
     */
    private static List<AbstractMap.SimpleImmutableEntry<Long, Long>> generateOffsetList(int size) {
        int segmentSize = 100;
        return LongStream.iterate(0L, offset -> offset + segmentSize)
                .limit(size)
                .mapToObj(offset -> new AbstractMap.SimpleImmutableEntry<Long, Long>(offset, offset + segmentSize - 1))
                .collect(Collectors.toList());
    }

    private List<ComparatorInfo.ComparatorReplicaInfo> generateReplicaInfo(Map<String, Path> tierStateFolderMap)
            throws IOException {
        for (Path tempDir : tierStateFolderMap.values()) {
            Path idPartitionADir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                    idPartitionA.topicPartition(), tempDir.toString()));
            Path idPartitionBDir = Files.createDirectory(TierMetadataValidator.getSnapshotFilePath(
                    idPartitionB.topicPartition(), tempDir.toString()));
            generateTierStateFile(idPartitionA, generateOffsetList(2), idPartitionADir);
            resetOffset();
            generateTierStateFile(idPartitionB, generateOffsetList(1), idPartitionBDir);
        }
        return TierMetadataComparator.getReplicas(idPartitionSet, tierStateFolderMap);
    }

    private FileTierPartitionState generateTierStateFile(final TopicIdPartition idPartition,
                                                         final List<AbstractMap.SimpleImmutableEntry<Long, Long>> startAndEndOffsets,
                                                         Path tierStateFolder) throws IOException {
        final FileTierPartitionState fileTierPartition = new FileTierPartitionState(tierStateFolder.toFile(),
                new LogDirFailureChannel(1), idPartition.topicPartition(), true);
        fileTierPartition.setTopicId(idPartition.topicId());
        fileTierPartition.onCatchUpComplete();
        assertEquals(TierPartitionState.AppendResult.ACCEPTED,
                fileTierPartition.append(new TierTopicInitLeader(idPartition, 0, UUID.randomUUID(), 0),
                        getNextOffset()));
        startAndEndOffsets.forEach(entry -> {
            final TierSegmentUploadInitiate uploadInit =
                    new TierSegmentUploadInitiate(idPartition, 0, UUID.randomUUID(), entry.getKey(), entry.getValue(),
                            1, 100, false, false, false, getNextOffset());
            assertEquals(TierPartitionState.AppendResult.ACCEPTED, fileTierPartition.append(uploadInit, uploadInit.stateOffsetAndEpoch()));
            try {
                if (writeDummySegmentFile(uploadInit)) {
                    // we are going to append the segment upload message only if the underlying segment files are created
                    assertEquals(TierPartitionState.AppendResult.ACCEPTED,
                            fileTierPartition.append(new TierSegmentUploadComplete(uploadInit), getNextOffset()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        // this is necessary to populate the tierstate file from the tierstate.mutable
        fileTierPartition.flush();
        // once flushed, we verify that the tierstate file exists and is of non-zero size
        Path generatedTierStateFile = tierStateFolder.resolve(TIER_STATE_FILE_NAME);
        assertTrue(Files.isRegularFile(generatedTierStateFile));
        assertTrue(Files.size(generatedTierStateFile) > 0);
        return fileTierPartition;
    }

    private boolean writeDummySegmentFile(TierSegmentUploadInitiate uploadInitiate) throws IOException {
        if (objStoreOpt.isPresent()) {
            File segmentFile = TierMetadataValidatorTest.generateDummyTempFiles(uploadInitiate.objectIdAsBase64(),
                    TierObjectStore.FileType.SEGMENT, uploadInitiate.size());
            File offsetIndexFile = TierMetadataValidatorTest.generateDummyTempFiles(uploadInitiate.objectIdAsBase64(),
                    TierObjectStore.FileType.OFFSET_INDEX, uploadInitiate.size());
            File timestampIndexFile = TierMetadataValidatorTest.generateDummyTempFiles(uploadInitiate.objectIdAsBase64(),
                    TierObjectStore.FileType.TIMESTAMP_INDEX, uploadInitiate.size());
            TierObjectStore.ObjectMetadata metadata = new TierObjectStore.ObjectMetadata(uploadInitiate.topicIdPartition(),
                    uploadInitiate.objectId(), uploadInitiate.tierEpoch(), uploadInitiate.baseOffset(),
                    uploadInitiate.hasAbortedTxns(), uploadInitiate.hasProducerState(), uploadInitiate.hasEpochState());
            objStoreOpt.get().putSegment(metadata, segmentFile, offsetIndexFile, timestampIndexFile,
                    Optional.empty(), Optional.empty(), Optional.empty());
            return true;
        }
        return false;
    }

    private AtomicLong revolvingOffset;

    private synchronized OffsetAndEpoch getNextOffset() {
        return new OffsetAndEpoch(revolvingOffset.incrementAndGet(), Optional.of(0));
    }

    private synchronized void resetOffset() {
        revolvingOffset.set(-1L);
    }
}
