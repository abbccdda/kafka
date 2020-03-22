/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import kafka.server.KafkaConfig;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierObjectMetadata.State;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStore.FileType;
import kafka.tier.store.TierObjectStore.ObjectMetadata;
import kafka.tier.store.TierObjectStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TierMetadataValidatorTest {
    List<TierObjectMetadata> aList = new ArrayList<>();
    List<TierObjectMetadata> eList = new ArrayList<>();
    TopicIdPartition tid = new TopicIdPartition("a1", UUID.randomUUID(), 0);
    Iterator<TierObjectMetadata> aIterator;
    Iterator<TierObjectMetadata> eIterator;
    TierMetadataValidator validator;
    TierObjectStore objStore;
    TierTopicMaterializationUtils mockUtils;

    @Before
    public void setup() throws IOException {
        // Basic test when all the state are same.
        aList.add(new TierObjectMetadata(tid, 0, UUID.randomUUID(), 0, 1000, 1, 1000,
            State.SEGMENT_UPLOAD_COMPLETE, false, false, false));
        aList.add(new TierObjectMetadata(tid, 0, UUID.randomUUID(), 1001, 2000, 1, 1000,
            State.SEGMENT_UPLOAD_COMPLETE, false, false, false));
        aList.add(new TierObjectMetadata(tid, 0, UUID.randomUUID(), 2001, 3000, 1, 1000,
            State.SEGMENT_UPLOAD_COMPLETE, false, false, false));

        eList.add(new TierObjectMetadata(tid, 0, aList.get(0).objectId(), 0, 1000, 1, 1000,
            State.SEGMENT_UPLOAD_COMPLETE, false, false, false));
        eList.add(new TierObjectMetadata(tid, 0, aList.get(1).objectId(), 1001, 2000, 1, 1000,
            State.SEGMENT_UPLOAD_COMPLETE, false, false, false));
        eList.add(new TierObjectMetadata(tid, 0, aList.get(2).objectId(), 2001, 3000, 1, 1000,
            State.SEGMENT_UPLOAD_COMPLETE, false, false, false));

        objStore = TierObjectStoreFactory.getObjectStoreInstance(TierObjectStore.Backend.Mock,
                new TierObjectStoreConfig("mock_cluster", 42));

        for (TierObjectMetadata tierMetadata : aList) {
            ObjectMetadata metadata = new ObjectMetadata(tierMetadata);
            File segmentFile = generateDummyTempFiles(tierMetadata.objectIdAsBase64(), FileType.SEGMENT, tierMetadata.size());
            File offsetIndexFile = generateDummyTempFiles(tierMetadata.objectIdAsBase64(), FileType.OFFSET_INDEX, tierMetadata.size());
            File timestampIndexFile = generateDummyTempFiles(tierMetadata.objectIdAsBase64(), FileType.TIMESTAMP_INDEX, tierMetadata.size());
            objStore.putSegment(metadata, segmentFile, offsetIndexFile, timestampIndexFile, Optional.empty(), Optional.empty(), Optional.empty());
        }


        aIterator = aList.iterator();
        eIterator = eList.iterator();

        String[] args = {
                "--metadata-states-dir", "/mnt/kafka",
                "--validate-tier-storage", "true",
                "--confluent.tier.backend", "Mock",
                "--cluster-id", "mock_cluster",
                "--broker.id", "42"
        };
        validator = new TierMetadataValidator(args);
        mockUtils = mock(TierTopicMaterializationUtils.class);
        when(mockUtils.getStartOffset(any())).thenReturn(0L);
        validator.utils = mockUtils;
    }

    @After
    public void teardown() {
        validator.close();
    }

    private File generateDummyTempFiles(String fileName, FileType type, long size)
        throws IOException {
        File tempFile = File.createTempFile(fileName, "." + type.suffix());
        byte[] buffer = new byte[4 * (int) size];
        try (FileOutputStream stream = new FileOutputStream(tempFile)) {
            stream.write(buffer);
        }
        tempFile.deleteOnExit();
        return tempFile;
    }

    @Test
    public void testTierMetadataValidatorTest() {
        String[] args = {
                "--metadata-states-dir", "/mnt/kafka",
                "--working-dir", "/tmp/rohit",
                "--bootstrap-server", "localhost:7099",
                "--tier-state-topic-partition", "10",
                "--snapshot-states-file", "true",
                "--confluent.tier.backend", "Mock",
                "--cluster-id", "mock_cluster",
                "--broker.id", "42"
        };

        TierMetadataValidator validator = new TierMetadataValidator(args);
        assertEquals(validator.props.getProperty(TierTopicMaterializationToolConfig.METADATA_STATES_DIR), "/mnt/kafka");
        assertEquals(validator.workDir, "/tmp/rohit");
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG), "localhost:7099");
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION), new Integer(10));
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES), true);
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.TIER_STORAGE_VALIDATION), true);
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.TIER_STORAGE_OFFSET_VALIDATION), false);
        assertEquals(validator.props.get(KafkaConfig.TierBackendProp()), TierObjectStore.Backend.Mock);
        assertEquals(validator.props.get(KafkaConfig.BrokerIdProp()), 42);
        assertEquals(validator.props.getProperty(TierTopicMaterializationToolConfig.CLUSTER_ID), "mock_cluster");
    }

    @Test
    public void basicValidateStatesTest() {
        assertTrue(validator.isValidStates(aIterator, eIterator, 0));
    }

    @Test
    public void validationFailOnVoidOffsetRange()  {
        TierObjectMetadata obj = eList.get(0);
        eList.set(0, new TierObjectMetadata(obj.topicIdPartition(), obj.tierEpoch(), obj.objectId(),
            obj.baseOffset() + 1, obj.endOffset(), obj.maxTimestamp(), obj.size(), obj.state(),
            false, false, false));
        aList.set(0, eList.get(0));
        aIterator = aList.iterator();
        eIterator = eList.iterator();
        assertFalse(validator.isValidStates(aIterator, eIterator, 0));
    }

    @Test
    public void validationPassOnVoidOffsetRangeBeforeStartOffset()  {
        TierObjectMetadata obj = eList.get(0);
        eList.set(0, new TierObjectMetadata(obj.topicIdPartition(), obj.tierEpoch(), obj.objectId(),
            obj.baseOffset() + 1, obj.endOffset(), obj.maxTimestamp(), obj.size(), obj.state(),
            false, false, false));
        aList.set(0, eList.get(0));
        aIterator = aList.iterator();
        eIterator = eList.iterator();
        assertTrue(validator.isValidStates(aIterator, eIterator, 1001));

        // Same test should fail if firstValidOffset is found in object with a hole.
        aIterator = aList.iterator();
        eIterator = eList.iterator();
        assertFalse(validator.isValidStates(aIterator, eIterator, 501));
    }

    @Test
    public void validationPassOnFencedMappingInActiveRange() {
        TierObjectMetadata obj = eList.get(2);
        eList.set(2, new TierObjectMetadata(obj.topicIdPartition(), obj.tierEpoch(), obj.objectId(),
            obj.baseOffset(), obj.endOffset(), obj.maxTimestamp(), obj.size(), State.SEGMENT_FENCED,
            false, false, false));
        aList.set(2, new TierObjectMetadata(obj.topicIdPartition(), obj.tierEpoch(), obj.objectId(),
            obj.baseOffset(), obj.endOffset(), obj.maxTimestamp(), obj.size(), State.SEGMENT_FENCED,
            false, false, false));
        aList.add(obj);
        eList.add(obj);
        aIterator = aList.iterator();
        eIterator = eList.iterator();
        assertTrue(validator.isValidStates(aIterator, eIterator, 0));
    }

    @Test
    public void testOffsetScanThrowsWithMockBackend() {
        String[] args = {
                "--metadata-states-dir", "/mnt/kafka",
                "--working-dir", "/tmp/rohit",
                "--bootstrap-server", "localhost:7099",
                "--tier-state-topic-partition", "10",
                "--snapshot-states-file", "true",
                "--confluent.tier.backend", "Mock",
                "--cluster-id", "mock_cluster",
                "--broker.id", "42",
                "--validate-tier-storage-offset", "true"
        };
        Exception caught = assertThrows(IllegalArgumentException.class, () -> new TierMetadataValidator(args));
        String actualMsg = caught.getMessage();
        String expectedMsg = "Unsupported backend for offset scan: " + TierObjectStore.Backend.Mock;

        assertTrue(actualMsg.contains(expectedMsg));
    }

    @Test
    public void testObjectStoreIgnoresInactiveSegment() {
        TierObjectMetadata deletedMetadata = new TierObjectMetadata(tid, 0, UUID.randomUUID(), 41, 50, 1, 10,
                State.SEGMENT_DELETE_COMPLETE, false, false, false);
        when(mockUtils.getStartOffset(any())).thenReturn(deletedMetadata.endOffset() + 1);
        final TierMetadataValidator.OffsetValidationResult validationResult = validator.verifyObjectInBackend(deletedMetadata, 0, objStore, false);
        assertTrue(validationResult.result);
        assertEquals(deletedMetadata.endOffset() + 1, validationResult.firstValidOffset);
    }

    @Test
    public void testObjectStoreIgnoresFencedSegment() {
        TierObjectMetadata deletedMetadata = new TierObjectMetadata(tid, 0, UUID.randomUUID(), 41, 50, 1, 10,
                State.SEGMENT_FENCED, false, false, false);
        when(mockUtils.getStartOffset(any())).thenReturn(20L);
        TierMetadataValidator.OffsetValidationResult validationResult = validator.verifyObjectInBackend(deletedMetadata, 0, objStore, false);
        assertTrue(validationResult.result);
    }

    @Test
    public void testNonExistentObject() {
        TierObjectMetadata nonExistentMetadata = new TierObjectMetadata(tid, 0, UUID.randomUUID(), 41, 50, 1, 10,
                State.SEGMENT_UPLOAD_COMPLETE, false, false, false);
        assertFalse(validator.verifyObjectInBackend(nonExistentMetadata, 0, objStore, false).result);
    }

    @Test
    public void testOffsetScanFailsWithMockBackend() {
        TierMetadataValidator.OffsetValidationResult validationResult = validator.verifyObjectInBackend(aList.get(0), 0, objStore, true);
        assertFalse(validationResult.result);
    }
}
