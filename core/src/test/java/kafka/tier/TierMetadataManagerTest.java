/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.log.LogConfig;
import kafka.server.LogDirFailureChannel;
import kafka.tier.state.TierPartitionStatus;
import kafka.tier.store.TierObjectStoreConfig;
import kafka.tier.state.FileTierPartitionStateFactory;
import kafka.tier.state.TierPartitionState;
import kafka.tier.store.MockInMemoryTierObjectStore;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TierMetadataManagerTest {
    private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(
            "myTopic", UUID.randomUUID(), 0);
    private static final TierObjectStoreConfig OBJECT_STORE_CONFIG = new TierObjectStoreConfig();
    private final File dir = TestUtils.tempDirectory();
    private int onBecomeLeader = 0;
    private int onBecomeFollower = 0;
    private int onDelete = 0;

    @After
    public void tearDown() throws IOException {
        Utils.delete(dir);
    }

    @Test
    public void testInitStateForTierEnabledTopic() throws IOException {
        LogConfig config = config(true, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir,
                config);
        assertTrue(partitionState.tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().tieringEnabled());
        assertEquals(TierPartitionStatus.CLOSED, partitionState.status());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);

        partitionState.setTopicIdPartition(TOPIC_ID_PARTITION);
        partitionState.beginCatchup();
        assertEquals(TierPartitionStatus.CATCHUP, partitionState.status());
        partitionState.flush();

        metadataManager.close();

        // Test reopen metadata manager and tier partition state
        TierMetadataManager metadataManager2 = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager2);
        TierPartitionState partitionState2 = metadataManager2.initState(TOPIC_ID_PARTITION.topicPartition(), dir,
                config);
        assertEquals(TierPartitionStatus.CATCHUP, partitionState2.status());

        metadataManager2.delete(TOPIC_ID_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(1, onDelete);
        metadataManager.close();
    }

    @Test
    public void testInitStateForTierDisabledTopic() throws IOException {
        LogConfig config = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir, config);
        assertFalse(partitionState.tieringEnabled());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().tieringEnabled());
        assertEquals(TierPartitionStatus.CLOSED, partitionState.status());
        metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);

        // Test reopen metadata manager and tier partition state
        TierMetadataManager metadataManager2 = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager2);
        TierPartitionState partitionState2 = metadataManager2.initState(TOPIC_ID_PARTITION.topicPartition(), dir,
                config);
        assertEquals(TierPartitionStatus.CLOSED, partitionState2.status());

        metadataManager2.delete(TOPIC_ID_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);
        metadataManager.close();
    }

    @Test
    public void testInitStateForCompactedTopic() throws IOException {
        LogConfig config = config(true, true);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState state = metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir, config);
        assertFalse(state.tieringEnabled());
        metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);
    }

    @Test
    public void testInitStateForTierTopicWithTierFeatureDisabled() throws IOException {
        LogConfig config = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                false);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir,
                config);
        assertFalse(partitionState.tieringEnabled());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().tieringEnabled());
        assertEquals(TierPartitionStatus.CLOSED, partitionState.status());
        metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);
    }

    @Test
    public void testUpdateConfigTierEnableAsFollower() throws IOException {
        LogConfig oldConfig = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir,
                oldConfig);
        metadataManager.becomeFollower(TOPIC_ID_PARTITION);

        LogConfig newConfig = config(true, false);
        metadataManager.onConfigChange(TOPIC_ID_PARTITION.topicPartition(), newConfig);
        assertTrue(partitionState.tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().tieringEnabled());
        assertTrue(partitionState.status().isOpen());

        // disabling tiering should now throw an exception
        assertThrows(IllegalStateException.class, () ->
                metadataManager.onConfigChange(TOPIC_ID_PARTITION.topicPartition(), oldConfig));
        metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(1, onBecomeFollower);
        assertEquals(1, onDelete);
    }

    @Test
    public void testUpdateConfigTierEnableAsLeader() throws IOException {
        LogConfig oldConfig = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir,
                oldConfig);
        metadataManager.becomeLeader(TOPIC_ID_PARTITION, 0);

        LogConfig newConfig = config(true, false);
        metadataManager.onConfigChange(TOPIC_ID_PARTITION.topicPartition(), newConfig);
        assertTrue(partitionState.tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().tieringEnabled());
        assertTrue(partitionState.status().isOpen());

        // disabling tiering should now throw an exception
        assertThrows(IllegalStateException.class, () ->
                metadataManager.onConfigChange(TOPIC_ID_PARTITION.topicPartition(), oldConfig));
        metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());

        assertEquals(1, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(1, onDelete);
    }

    @Test
    public void testUpdateConfigTierEnableNoTopicId() throws IOException {
        LogConfig oldConfig = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir,
                oldConfig);

        LogConfig newConfig = config(true, false);
        metadataManager.onConfigChange(TOPIC_ID_PARTITION.topicPartition(), newConfig);
        assertTrue(partitionState.tieringEnabled());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).isPresent());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().tieringEnabled());
        assertFalse(partitionState.status().isOpen());

        // disabling tiering should now throw an exception
        assertThrows(IllegalStateException.class,
                    () -> metadataManager.onConfigChange(TOPIC_ID_PARTITION.topicPartition(),
                            oldConfig));

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals("onDelete should not be called as partition state does not have a topic ID"
                        + " associated and should thus not be wrritten to a file yet",
                0, onDelete);

        // subsequent become leader after config change
        metadataManager.becomeLeader(TOPIC_ID_PARTITION, 0);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().epochIfLeader().getAsInt(), 0);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().epochIfLeader().getAsInt(), 0);

        // become follower after config change
        metadataManager.becomeFollower(TOPIC_ID_PARTITION);
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().epochIfLeader().isPresent());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().epochIfLeader().isPresent());

        assertEquals(1, onBecomeLeader);
        assertEquals(1, onBecomeFollower);
        assertEquals(0, onDelete);

        metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());
    }

    @Test
    public void testUpdateConfigCompactEnable() throws IOException {
        LogConfig oldConfig = config(true, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir, oldConfig);
        TierPartitionState tierPartitionState = metadataManager.tierPartitionState(TOPIC_ID_PARTITION.topicPartition()).get();
        tierPartitionState.setTopicIdPartition(TOPIC_ID_PARTITION);

        LogConfig newConfig = config(false, true);
        try {
            // disabling tiering should now throw an exception
            assertThrows(IllegalStateException.class,
                    () -> metadataManager.onConfigChange(TOPIC_ID_PARTITION.topicPartition(),
                            newConfig));
        } finally {
            metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());
        }

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(1, onDelete);
    }

    @Test
    public void testBecomeLeaderAndBecomeFollower() throws IOException {
        LogConfig config = config(true, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        metadataManager.initState(TOPIC_ID_PARTITION.topicPartition(), dir, config);

        // become leader with epoch 0
        metadataManager.becomeLeader(TOPIC_ID_PARTITION, 0);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().epochIfLeader().getAsInt(), 0);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().epochIfLeader().getAsInt(), 0);

        // advance epoch to 1
        metadataManager.becomeLeader(TOPIC_ID_PARTITION, 1);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().epochIfLeader().getAsInt(), 1);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().epochIfLeader().getAsInt(), 1);

        // become follower
        metadataManager.becomeFollower(TOPIC_ID_PARTITION);
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().epochIfLeader().isPresent());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().epochIfLeader().isPresent());

        // become follower again
        metadataManager.becomeFollower(TOPIC_ID_PARTITION);
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().epochIfLeader().isPresent());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().epochIfLeader().isPresent());

        // now become leader with epoch 3
        metadataManager.becomeLeader(TOPIC_ID_PARTITION, 3);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).get().epochIfLeader().getAsInt(), 3);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).get().epochIfLeader().getAsInt(), 3);

        metadataManager.delete(TOPIC_ID_PARTITION.topicPartition());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION.topicPartition()).isPresent());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_ID_PARTITION).isPresent());

        assertEquals(3, onBecomeLeader);
        assertEquals(2, onBecomeFollower);
        assertEquals(1, onDelete);
    }

    @Test
    public void testTierEnabledLeaderPartitionStateIterator() throws IOException {
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Optional.of(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        LogConfig tierEnableConfig = config(true, false);
        LogConfig tierDisableConfig = config(false, false);

        TopicIdPartition partition1 = new TopicIdPartition("foo-1", UUID.randomUUID(), 0);
        TopicIdPartition partition2 = new TopicIdPartition("foo-2", UUID.randomUUID(), 0);
        TopicIdPartition partition3 = new TopicIdPartition("foo-3", UUID.randomUUID(), 0);
        TopicIdPartition partition4 = new TopicIdPartition("foo-4", UUID.randomUUID(), 0);

        metadataManager.initState(partition1.topicPartition(), dir, tierEnableConfig);
        metadataManager.becomeLeader(partition1, 0);

        metadataManager.initState(partition2.topicPartition(), dir, tierDisableConfig);
        metadataManager.becomeLeader(partition2, 0);

        metadataManager.initState(partition3.topicPartition(), dir, tierEnableConfig);
        metadataManager.becomeFollower(partition3);

        metadataManager.initState(partition4.topicPartition(), dir, tierDisableConfig);
        metadataManager.becomeFollower(partition4);

        List<TopicIdPartition> tierEnabledPartitions = new LinkedList<>();
        Iterator<TierPartitionState> it = metadataManager.tierEnabledPartitionStateIterator();
        while (it.hasNext())
            tierEnabledPartitions.add(it.next().topicIdPartition().get());

        List<TopicIdPartition> tierEnabledLeaderPartitions = new LinkedList<>();
        it = metadataManager.tierEnabledLeaderPartitionStateIterator();
        while (it.hasNext())
            tierEnabledLeaderPartitions.add(it.next().topicIdPartition().get());

        assertEquals(Arrays.asList(partition1, partition3), tierEnabledPartitions);
        assertEquals(Arrays.asList(partition1), tierEnabledLeaderPartitions);
    }

    private void addListener(TierMetadataManager metadataManager) {
        metadataManager.addListener(this.getClass(), new TierMetadataManager.ChangeListener() {
            @Override
            public void onBecomeLeader(TopicIdPartition topicIdPartition, int leaderEpoch) {
                onBecomeLeader++;
            }

            @Override
            public void onBecomeFollower(TopicIdPartition topicIdPartition) {
                onBecomeFollower++;
            }

            @Override
            public void onDelete(TopicIdPartition topicIdPartition) {
                onDelete++;
            }
        });
    }

    private LogConfig config(boolean tierEnable, boolean compactionEnable) {
        Properties props = new Properties();
        props.put(LogConfig.TierEnableProp(), tierEnable);
        props.put(LogConfig.CleanupPolicyProp(), compactionEnable ? LogConfig.Compact() : LogConfig.Delete());
        return new LogConfig(props, JavaConversions.asScalaSet(new HashSet<>()).toSet());
    }
}
