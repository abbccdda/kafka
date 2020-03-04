/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

public class TierMetadataValidatorTest {
    @Test
    public void TierMetadataValidatorTest() throws IOException {
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
}
