/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import java.util.Properties;
import java.util.UUID;
import kafka.utils.CoreUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class TierTopicMaterializationToolConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    public static final String SRC_TOPIC_ID = "source-topic-id";
    public static final String SRC_TOPIC_DOC = "source topic id. If selected all the processing will be limited for this id";

    public static final String SRC_PARTITION = "source-partition";
    public static final String SRC_PARTIITON_DOC = "source partition id. If selected, all the processing will be limited for this partition.";

    public static final String TIER_STATE_TOPIC_PARTITION = "tier-state-topic-partition";
    public static final String TIER_STATE_TOPIC_PARTITION_DOC = "tier topic partition. If selected all the processing will be limited from this partition.";

    public static final String START_OFFSET = "start-offset";
    public static final String START_OFFSET_DOC = "start offset from where the events will be processed." +
            "This will be ignored if tier-topic-partition is not set.";

    public static final String END_OFFSET = "end-offset";
    public static final String END_OFFSET_DOC = "end offset from where the events will be processed. This will be " +
            "ignored if tier-topic-partition is not set.";

    public static final String BOOTSTRAP_SERVER_CONFIG = "bootstrap-server";
    public static final String BOOTSTRAP_SERVER_DOC = "The broker server and port string in form host:port";

    public static final String DUMP_METADATA = "dump-metadata";
    public static final String DUMP_METADATA_DOC = "dump-metadata will dump entire metadata of tier topic partitions on the std output.";

    public static final String DUMP_METADATA_HEADER = "dump-metadata-header";
    public static final String DUMP_METADATA_HEADER_DOC = "dump-metadata-header will dump only header of the tier topic partition's state.";

    public static final String WORKING_DIR = "working-dir";
    public static final String WORKING_DIR_DOC = "working-dir is the directory path where the tool will generate its data";

    public static final String METADATA_STATES_DIR = "metadata-states-dir";
    public static final String METADATA_STATES_DIR_DOC = "data directory of kafka, to pull tier topic state files.";

    public static final String EMPTY_UUID_STR = "00000000-0000-0000-0000-000000000000";
    public static final UUID EMPTY_UUID = CoreUtils.uuidFromBase64(EMPTY_UUID_STR);

    public static final String DUMP_EVENTS = "dump-events";
    public static final String DUMP_EVENTS_DOC = "dumps the tier state topic's events on std out. By default its"
        + " turned to be true. If chosen false it will still dump everty 1000th events for tracking"
        + "long runs.";

    public static final String MATERIALIZE = "materialize";
    public static final String MATERIALIZE_DOC = "if set, then only materialize the events to generate "
        + "state file.";

    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVER_CONFIG,
                ConfigDef.Type.STRING,
                "localhost:9071",
                ConfigDef.Importance.HIGH,
                BOOTSTRAP_SERVER_DOC)
                .define(SRC_TOPIC_ID,
                        ConfigDef.Type.STRING,
                        EMPTY_UUID_STR,
                        ConfigDef.Importance.MEDIUM,
                        SRC_TOPIC_DOC)
                .define(SRC_PARTITION,
                        ConfigDef.Type.INT,
                        -1,
                        ConfigDef.Importance.LOW,
                        SRC_PARTIITON_DOC)
                .define(TIER_STATE_TOPIC_PARTITION,
                        ConfigDef.Type.INT,
                        -1,
                        ConfigDef.Importance.LOW,
                        TIER_STATE_TOPIC_PARTITION_DOC)
                .define(START_OFFSET,
                        ConfigDef.Type.INT,
                        0,
                        ConfigDef.Importance.MEDIUM,
                        START_OFFSET_DOC)
                .define(END_OFFSET,
                        ConfigDef.Type.INT,
                        -1,
                        ConfigDef.Importance.LOW,
                        END_OFFSET_DOC)
                .define(DUMP_METADATA,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.MEDIUM,
                        DUMP_METADATA_DOC)
                .define(DUMP_METADATA_HEADER,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.MEDIUM,
                        DUMP_METADATA_HEADER_DOC)
                .define(WORKING_DIR,
                        ConfigDef.Type.STRING,
                        "/tmp/workDir",
                        ConfigDef.Importance.HIGH,
                        WORKING_DIR_DOC)
                .define(METADATA_STATES_DIR,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        METADATA_STATES_DIR_DOC)
                .define(DUMP_EVENTS,
                        Type.BOOLEAN,
                        true,
                        Importance.MEDIUM,
                        DUMP_EVENTS_DOC)
                .define(MATERIALIZE,
                        Type.BOOLEAN,
                        true,
                        Importance.MEDIUM,
                        MATERIALIZE_DOC);
    }

    public final Integer userPartition;
    public final Integer partition;
    public final Integer startOffset;
    public final Integer endOffset;
    public final String server;
    public final Boolean dumpRecords;
    public final UUID userTopicId;
    public final String materializationPath;
    public final Boolean dumpHeader;
    public final String metadataStatesDir;
    public final Boolean dumpEvents;
    public final Boolean materialize;

    public TierTopicMaterializationToolConfig(Properties props) {
        super(CONFIG, props);
        System.out.println("Configs: " + props);
        userPartition = this.getInt(SRC_PARTITION);
        partition = this.getInt(TIER_STATE_TOPIC_PARTITION);
        server = this.getString(BOOTSTRAP_SERVER_CONFIG);
        dumpHeader = this.getBoolean(DUMP_METADATA_HEADER);
        dumpRecords = this.getBoolean(DUMP_METADATA);
        userTopicId = CoreUtils.uuidFromBase64(this.getString(SRC_TOPIC_ID));
        materializationPath = this.getString(WORKING_DIR);
        metadataStatesDir = this.getString(METADATA_STATES_DIR);
        dumpEvents = this.getBoolean(DUMP_EVENTS);
        materialize = this.getBoolean(MATERIALIZE);

        if (partition.equals(-1)) {
            // Ignore start and end offset if tier topic partition is not set.
            startOffset = 0;
            endOffset = -1;
        } else {
            startOffset = this.getInt(START_OFFSET);
            endOffset = this.getInt(END_OFFSET);
        }
    }
}
