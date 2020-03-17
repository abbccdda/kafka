/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierObjectMetadata.State;
import org.junit.Before;
import org.junit.Test;

public class TierMetadataValidatorTest {
    List<TierObjectMetadata> aList = new ArrayList<>();
    List<TierObjectMetadata> eList = new ArrayList<>();
    TopicIdPartition tid = new TopicIdPartition("a1", UUID.randomUUID(), 0);
    Iterator<TierObjectMetadata> aIterator;
    Iterator<TierObjectMetadata> eIterator;
    TierMetadataValidator validator;

    @Before public void setup() {
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

        aIterator = aList.iterator();
        eIterator = eList.iterator();

        String[] args = {
            "--metadata-states-dir", "/mnt/kafka"
        };
        validator = new TierMetadataValidator(args);
    }
    @Test
    public void TierMetadataValidatorTest() {
        String[] args = {
                "--metadata-states-dir", "/mnt/kafka",
                "--working-dir", "/tmp/rohit",
                "--bootstrap-server", "localhost:7099",
                "--tier-state-topic-partition", "10",
                "--snapshot-states-file", "true"
        };

        TierMetadataValidator validator = new TierMetadataValidator(args);
        assertEquals(validator.props.getProperty(TierTopicMaterializationToolConfig.METADATA_STATES_DIR), "/mnt/kafka");
        assertEquals(validator.workDir, "/tmp/rohit");
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG), "localhost:7099");
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION), new Integer(10));
        assertEquals(validator.props.get(TierTopicMaterializationToolConfig.SNAPSHOT_STATES_FILES), new Boolean(true));
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
}
