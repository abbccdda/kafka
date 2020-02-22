/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import java.io.IOException;
import java.util.Properties;
//CHECKSTYLE:OFF
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
//CHECKSTYLE:ON
import org.apache.kafka.common.errors.AuthenticationException;


/**
 * TierMetadataDebugger is a tool to dump raw metadata events of TierTopicPartitions, additionally it can also materialize and
 * generate expected state.
 *
 * Usage:
 * # Dumps and generates metadata states for all the tier topic partition. By default, the working directory will be
 * # /tmp/working-dir.
 * $KAFKA_BIN/kafka-tier-materialization-debugger.sh
 *
 * # Dumps and generate metadata states for desired source topic with optional partition.
 * $KAFKA_BIN/kafka-tier-materialization-debugger.sh --source-topic-id=(Base64 string representation of UUID).
 *
 * # Dumps and generate metadata states from a given tier topic partition with option of start and end offset and source topic.
 * $KAFKA_BIN/kafka-tier-materialization-debugger.sh --tier-topic-partition=5 --start-offset=50 --end-offset=100
 *
 */
public class TierMetadataDebugger {
    public static void main(String[] args) {
        try {
            Properties props = fetchPropertiesFromArgs(args);
            TierTopicMaterializationToolConfig config = new TierTopicMaterializationToolConfig(props);
            TierTopicMaterializationUtils consumer = new TierTopicMaterializationUtils(config);
            consumer.run();
        } catch (AuthenticationException ae) {
            System.out.println(ae.getMessage());
            ae.printStackTrace();
            System.exit(1);
        } catch (IOException ie) {
            System.out.println(ie.getMessage());
            ie.printStackTrace();
            System.exit(1);
        }
    }

    public static Properties fetchPropertiesFromArgs(String[] args) {

        // TBD need to validate arguments and various allowed permutation and combinations plus help and documentation.

        OptionParser parser = new OptionParser();

        OptionSpec<String> userTopicIdSpec = parser.accepts(TierTopicMaterializationToolConfig.SRC_TOPIC_ID,
                    TierTopicMaterializationToolConfig.SRC_TOPIC_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.SRC_TOPIC_ID)
                .ofType(String.class)
                .defaultsTo("00000000-0000-0000-0000-000000000000");

        OptionSpec<Integer> userPartitionSpec = parser.accepts(TierTopicMaterializationToolConfig.SRC_PARTITION,
                    TierTopicMaterializationToolConfig.SRC_PARTIITON_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.SRC_PARTITION)
                .ofType(Integer.class)
                .defaultsTo(-1);

        OptionSpec<Integer> partitionSpec = parser.accepts(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION,
                    TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION)
                .ofType(Integer.class)
                .defaultsTo(-1);

        OptionSpec<Integer> startOffsetSpec = parser.accepts(TierTopicMaterializationToolConfig.START_OFFSET,
                    TierTopicMaterializationToolConfig.START_OFFSET_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.START_OFFSET)
                .ofType(Integer.class)
                .defaultsTo(0);

        OptionSpec<Integer> endOffsetSpec = parser.accepts(TierTopicMaterializationToolConfig.END_OFFSET,
                    TierTopicMaterializationToolConfig.END_OFFSET_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.END_OFFSET)
                .ofType(Integer.class)
                .defaultsTo(-1);

        OptionSpec<String> bootstrapServerSpec = parser.accepts(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG,
                    TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG)
                .ofType(String.class)
                .defaultsTo("localhost:9092");

        OptionSpec<Boolean> dumpSpec = parser.accepts(TierTopicMaterializationToolConfig.DUMP_METADATA,
                    TierTopicMaterializationToolConfig.DUMP_METADATA_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.DUMP_METADATA)
                .ofType(Boolean.class)
                .defaultsTo(false);

        OptionSpec<Boolean> dumpHeaderSpec = parser.accepts(TierTopicMaterializationToolConfig.DUMP_METADATA_HEADER,
                    TierTopicMaterializationToolConfig.DUMP_METADATA_HEADER_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.DUMP_METADATA_HEADER)
                .ofType(Boolean.class)
                .defaultsTo(false);

        OptionSpec<String> workingDirSpec = parser.accepts(TierTopicMaterializationToolConfig.WORKING_DIR,
                    TierTopicMaterializationToolConfig.WORKING_DIR_DOC)
                .withRequiredArg()
                .describedAs(TierTopicMaterializationToolConfig.WORKING_DIR_DOC)
                .ofType(String.class)
                .defaultsTo("/tmp/workDir");

        OptionSpec<Boolean> materializeSpec = parser.accepts(TierTopicMaterializationToolConfig.MATERIALIZE,
                    TierTopicMaterializationToolConfig.MATERIALIZE_DOC)
                .withOptionalArg()
                .describedAs(TierTopicMaterializationToolConfig.WORKING_DIR_DOC)
                .ofType(Boolean.class)
                .defaultsTo(false);

        OptionSet options = parser.parse(args);

        Properties props = new Properties();
        props.put(TierTopicMaterializationToolConfig.SRC_PARTITION, options.valueOf(userPartitionSpec));
        props.put(TierTopicMaterializationToolConfig.SRC_TOPIC_ID, options.valueOf(userTopicIdSpec));
        props.put(TierTopicMaterializationToolConfig.TIER_STATE_TOPIC_PARTITION, options.valueOf(partitionSpec));
        props.put(TierTopicMaterializationToolConfig.START_OFFSET, options.valueOf(startOffsetSpec));
        props.put(TierTopicMaterializationToolConfig.END_OFFSET, options.valueOf(endOffsetSpec));
        props.put(TierTopicMaterializationToolConfig.BOOTSTRAP_SERVER_CONFIG, options.valueOf(bootstrapServerSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_METADATA, options.valueOf(dumpSpec));
        props.put(TierTopicMaterializationToolConfig.DUMP_METADATA_HEADER, options.valueOf(dumpHeaderSpec));
        props.put(TierTopicMaterializationToolConfig.WORKING_DIR, options.valueOf(workingDirSpec));
        props.put(TierTopicMaterializationToolConfig.MATERIALIZE, options.valueOf(materializeSpec));

        return props;
    }
}
