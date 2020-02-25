/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import java.util.Properties;
import java.util.UUID;
import kafka.utils.CoreUtils;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class TierTopicMaterializationUtilsTest {

    @Test
    public void TierTopicMaterializationToolTest() {
        UUID uuid = UUID.randomUUID();

        Properties props = new Properties();
        props.put(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG, "localhost:9092");
        props.put(TierTopicMaterializationToolConfig.SRC_TOPIC_ID, CoreUtils.uuidToBase64(uuid));
        props.put(TierTopicMaterializationToolConfig.SRC_PARTITION, "10");
        props.put(TierTopicMaterializationToolConfig.START_OFFSET, "4");
        props.put(TierTopicMaterializationToolConfig.WORKING_DIR, "/tmp/path");

        TierTopicMaterializationToolConfig config = new TierTopicMaterializationToolConfig(props);
        TierTopicMaterializationUtils tool = new TierTopicMaterializationUtils(config);
        assertEquals(tool.config.materializationPath, "/tmp/path");
        assertEquals(tool.config.userPartition, new Integer(10));
        assertEquals(tool.config.userTopicId, uuid);
        assertEquals(tool.config.endOffset,  new Integer(-1));
        assertEquals(tool.config.dumpHeader, false);
    }

    @Test
    public void TierTopicMaterializationToolSetupTest() {
        String[] args = {
                "--source-partition", "10",
                "--start-offset", "5"
        };

        Properties props = TierMetadataDebugger.fetchPropertiesFromArgs(args);
        System.out.println(props);
        System.out.println(TierTopicMaterializationToolConfig.SRC_PARTITION);
        TierTopicMaterializationUtils consumer = new TierTopicMaterializationUtils(new TierTopicMaterializationToolConfig(props));
        // Should not allow start offset if tier-state-topic-partition not set.
        assertEquals(consumer.config.startOffset, new Integer(0));
        assertEquals(consumer.config.userPartition, new Integer(10));
        assertEquals(consumer.config.userTopicId, TierTopicMaterializationToolConfig.EMPTY_UUID);
    }
}
