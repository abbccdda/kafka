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
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TierMetadataManagerTest {
    private static final TopicIdPartition TOPIC_PARTITION = new TopicIdPartition(
            "myTopic", UUID.randomUUID(), 0);
    private static final TierObjectStoreConfig OBJECT_STORE_CONFIG = new TierObjectStoreConfig();
    private final File dir = TestUtils.tempDirectory();
    private int onBecomeLeader = 0;
    private int onBecomeFollower = 0;
    private int onDelete = 0;

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(dir.toPath());
    }

    @Test
    public void testInitStateForTierEnabledTopic() throws IOException {
        LogConfig config = config(true, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir,
                config);
        assertTrue(partitionState.tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().tieringEnabled());
        assertEquals(TierPartitionStatus.CLOSED, partitionState.status());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);

        partitionState.setTopicIdPartition(TOPIC_PARTITION);
        partitionState.beginCatchup();
        assertEquals(TierPartitionStatus.CATCHUP, partitionState.status());
        partitionState.flush();

        metadataManager.close();

        // Test reopen metadata manager and tier partition state
        TierMetadataManager metadataManager2 = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager2);
        TierPartitionState partitionState2 = metadataManager2.initState(TOPIC_PARTITION.topicPartition(), dir,
                config);
        assertEquals(TierPartitionStatus.CATCHUP, partitionState2.status());

        metadataManager2.delete(TOPIC_PARTITION.topicPartition());

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
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir, config);
        assertFalse(partitionState.tieringEnabled());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().tieringEnabled());
        assertEquals(TierPartitionStatus.CLOSED, partitionState.status());
        metadataManager.delete(TOPIC_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);

        // Test reopen metadata manager and tier partition state
        TierMetadataManager metadataManager2 = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager2);
        TierPartitionState partitionState2 = metadataManager2.initState(TOPIC_PARTITION.topicPartition(), dir,
                config);
        assertEquals(TierPartitionStatus.CLOSED, partitionState2.status());

        metadataManager2.delete(TOPIC_PARTITION.topicPartition());

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
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState state = metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir, config);
        assertFalse(state.tieringEnabled());
        metadataManager.delete(TOPIC_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);
    }

    @Test
    public void testInitStateForTierTopicWithTierFeatureDisabled() throws IOException {
        LogConfig config = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                false);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir,
                config);
        assertFalse(partitionState.tieringEnabled());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().tieringEnabled());
        assertEquals(TierPartitionStatus.CLOSED, partitionState.status());
        metadataManager.delete(TOPIC_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(0, onDelete);
    }

    @Test
    public void testUpdateConfigTierEnableAsFollower() throws IOException {
        LogConfig oldConfig = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir,
                oldConfig);
        metadataManager.becomeFollower(TOPIC_PARTITION);

        LogConfig newConfig = config(true, false);
        metadataManager.onConfigChange(TOPIC_PARTITION.topicPartition(), newConfig);
        assertTrue(partitionState.tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().tieringEnabled());
        assertTrue(partitionState.status().isOpen());

        // disabling tiering should now throw an exception
        try {
            metadataManager.onConfigChange(TOPIC_PARTITION.topicPartition(), oldConfig);
            fail();
        } catch (InvalidConfigurationException e) {
        }
        metadataManager.delete(TOPIC_PARTITION.topicPartition());

        assertEquals(0, onBecomeLeader);
        assertEquals(1, onBecomeFollower);
        assertEquals(1, onDelete);
    }

    @Test
    public void testUpdateConfigTierEnableAsLeader() throws IOException {
        LogConfig oldConfig = config(false, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        TierPartitionState partitionState = metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir,
                oldConfig);
        metadataManager.becomeLeader(TOPIC_PARTITION, 0);

        LogConfig newConfig = config(true, false);
        metadataManager.onConfigChange(TOPIC_PARTITION.topicPartition(), newConfig);
        assertTrue(partitionState.tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().tieringEnabled());
        assertTrue(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().tieringEnabled());
        assertTrue(partitionState.status().isOpen());

        // disabling tiering should now throw an exception
        try {
            metadataManager.onConfigChange(TOPIC_PARTITION.topicPartition(), oldConfig);
            fail();
        } catch (InvalidConfigurationException e) {
        }
        metadataManager.delete(TOPIC_PARTITION.topicPartition());

        assertEquals(1, onBecomeLeader);
        assertEquals(0, onBecomeFollower);
        assertEquals(1, onDelete);
    }

    @Test
    public void testUpdateConfigCompactEnable() throws IOException {
        LogConfig oldConfig = config(true, false);
        TierMetadataManager metadataManager = new TierMetadataManager(
                new FileTierPartitionStateFactory(),
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir, oldConfig);
        TierPartitionState tierPartitionState = metadataManager.tierPartitionState(TOPIC_PARTITION.topicPartition()).get();
        tierPartitionState.setTopicIdPartition(TOPIC_PARTITION);

        LogConfig newConfig = config(false, true);
        try {
            metadataManager.onConfigChange(TOPIC_PARTITION.topicPartition(), newConfig);
            fail();
        } catch (InvalidConfigurationException e) {
        } finally {
            metadataManager.delete(TOPIC_PARTITION.topicPartition());
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
                Option.apply(new MockInMemoryTierObjectStore(OBJECT_STORE_CONFIG)),
                new LogDirFailureChannel(10),
                true);
        addListener(metadataManager);
        metadataManager.initState(TOPIC_PARTITION.topicPartition(), dir, config);

        // become leader with epoch 0
        metadataManager.becomeLeader(TOPIC_PARTITION, 0);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().epochIfLeader().getAsInt(), 0);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().epochIfLeader().getAsInt(), 0);

        // advance epoch to 1
        metadataManager.becomeLeader(TOPIC_PARTITION, 1);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().epochIfLeader().getAsInt(), 1);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().epochIfLeader().getAsInt(), 1);

        // become follower
        metadataManager.becomeFollower(TOPIC_PARTITION);
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().epochIfLeader().isPresent());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().epochIfLeader().isPresent());

        // become follower again
        metadataManager.becomeFollower(TOPIC_PARTITION);
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().epochIfLeader().isPresent());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().epochIfLeader().isPresent());

        // now become leader with epoch 3
        metadataManager.becomeLeader(TOPIC_PARTITION, 3);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).get().epochIfLeader().getAsInt(), 3);
        assertEquals(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).get().epochIfLeader().getAsInt(), 3);

        metadataManager.delete(TOPIC_PARTITION.topicPartition());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION.topicPartition()).isPresent());
        assertFalse(metadataManager.tierPartitionMetadata(TOPIC_PARTITION).isPresent());

        assertEquals(3, onBecomeLeader);
        assertEquals(2, onBecomeFollower);
        assertEquals(1, onDelete);
    }

    private void addListener(TierMetadataManager metadataManager) {
        metadataManager.addListener(new TierMetadataManager.ChangeListener() {
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
