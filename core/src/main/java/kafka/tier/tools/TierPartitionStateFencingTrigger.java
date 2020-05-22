/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import kafka.server.KafkaConfig;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierPartitionFence;
import kafka.tier.tools.common.FenceEventInfo;
import kafka.tier.topic.TierTopic;

import kafka.utils.CoreUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A tool that injects PartitionFence events into TierTopic for a provided list of
 * TopicIdPartition. This is helpful in triggering fencing of broker's TierPartitionState
 * materializer. The tool outputs to STDOUT a list of JSON-formatted
 * `kafka.tier.tools.common.FenceEventInfo` objects, each containing information about an injected
 * PartitionFence event.
 *
 * SAMPLE USAGE:
 * $> bin/kafka-run-class.sh \
 *     kafka.tier.tools.TierPartitionStateFencingTrigger \
 *        --tier.config /path/to/xxx.properties \
 *        --file-fence-target-partitions /path/to/fence_target_partitions.csv
 */
public class TierPartitionStateFencingTrigger {
    public static final List<String> REQUIRED_PROPERTIES = Arrays.asList(
        KafkaConfig.TierMetadataBootstrapServersProp(),
        KafkaConfig.TierMetadataNamespaceProp());

    public static final String FILE_FENCE_TARGET_PARTITIONS_CONFIG = "file-fence-target-partitions";
    public static final String FILE_FENCE_TARGET_PARTITIONS_DOC =
        "The path to a file containing non-empty list of target tiered partitions to be fenced by" +
        " the tool. The format of the file is a newline separated list of information. Each line" +
        " is a comma-separated value (CSV) containing information about a single tiered" +
        " TopicIdPartition in the following format:" +
        " '<tiered_partition_topic_ID_base64_encoded>, <tiered_partition_topic_name>, <tiered_partition_name>'.";

    private static final Logger log = LoggerFactory.getLogger(TierPartitionStateFencingTrigger.class);

    // Create the CLI argument parser.
    private static ArgumentParser createArgParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser(TierPartitionStateFencingTrigger.class.getName())
            .defaultHelp(true)
            .description("Provides a command to fence TierTopic using the TierPartitionFence event.");
        parser.addArgument(RecoveryUtils.makeArgument(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG))
            .dest(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG)
            .type(String.class)
            .required(true)
            .help(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_DOC);
        parser.addArgument(RecoveryUtils.makeArgument(TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG))
            .dest(TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG)
            .type(String.class)
            .required(true)
            .help(TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_DOC);

        return parser;
    }

    // Main entry point for the CLI tool. This picks the sub-command logic to run.
    private static List<FenceEventInfo> run(ArgumentParser parser, Namespace args)
        throws ArgumentParserException, InterruptedException, ExecutionException {
        final String propertiesConfFile =
            args.getString(RecoveryUtils.TIER_PROPERTIES_CONF_FILE_CONFIG).trim();
        final Properties props;
        try {
            props = Utils.loadProps(propertiesConfFile, REQUIRED_PROPERTIES);
        } catch (IOException e) {
            throw new ArgumentParserException(
                String.format("Can not load properties from file: '%s'", propertiesConfFile),
                e,
                parser);
        }

        final String bootstrapServers = props.getProperty(
            KafkaConfig.TierMetadataBootstrapServersProp(), "").trim();
        if (bootstrapServers.isEmpty()) {
            throw new ArgumentParserException(
                String.format(
                    "The provided properties conf file: '%s' can not contain empty or absent" +
                    " bootstrap servers as value for the property: '%s'",
                    propertiesConfFile,
                    KafkaConfig.TierMetadataBootstrapServersProp()),
                parser);
        }
        final String tierTopicNamespace =
            props.getProperty(KafkaConfig.TierMetadataNamespaceProp(), "");
        final String tieredTopicIdPartitionFile = args.getString(
            TierPartitionStateFencingTrigger.FILE_FENCE_TARGET_PARTITIONS_CONFIG).trim();
        final List<String> tieredTopicIdPartitionsStr;
        final List<TopicIdPartition> tieredTopicIdPartitions;
        try {
            Path filePath = Paths.get(tieredTopicIdPartitionFile);
            tieredTopicIdPartitionsStr = Files.readAllLines(filePath);
            tieredTopicIdPartitions = RecoveryUtils.toTopicIdPartitions(tieredTopicIdPartitionsStr);
        } catch (Exception e) {
            throw new ArgumentParserException(
                String.format(
                    "Can not parse partitions information from file: '%s'", tieredTopicIdPartitionFile),
                e,
                parser);
        }

        if (tieredTopicIdPartitions.isEmpty()) {
            throw new ArgumentParserException(
                String.format(
                    "Found no partitions information in file: '%s'", tieredTopicIdPartitionFile),
                parser);
        }

        log.info(
            "Read the following tiered TopicIdPartition from {} as candidates for fencing:\n{}\n",
            tieredTopicIdPartitionFile,
            String.join("\n", tieredTopicIdPartitionsStr));
        return injectFencingEvents(bootstrapServers, tierTopicNamespace, tieredTopicIdPartitions);
    }

    public static List<FenceEventInfo> injectFencingEvents(
        String bootstrapServers,
        String tierTopicNamespace,
        List<TopicIdPartition> tieredTopicIdPartitions) throws ExecutionException, InterruptedException {
        final String tierTopicName = TierTopic.topicName(tierTopicNamespace);
        final List<FenceEventInfo> events = new ArrayList<>();
        Producer<byte[], byte[]> producer = null;
        try {
            short numTierTopicPartitions = RecoveryUtils.getNumPartitions(
                bootstrapServers, tierTopicName);
            producer = RecoveryUtils.createTierTopicProducer(
                bootstrapServers,
                tierTopicNamespace,
                numTierTopicPartitions,
                TierPartitionStateFencingTrigger.class.getSimpleName());
            for (TopicIdPartition tieredPartition : tieredTopicIdPartitions) {
                final TierPartitionFence fencingEvent = new TierPartitionFence(
                    tieredPartition, UUID.randomUUID());
                final RecordMetadata metadata = RecoveryUtils.injectTierTopicEvent(
                    producer, fencingEvent, tierTopicName, numTierTopicPartitions);
                events.add(
                    new FenceEventInfo(
                        tieredPartition.topic(),
                        tieredPartition.topicIdAsBase64(),
                        tieredPartition.partition(),
                        CoreUtils.uuidToBase64(fencingEvent.messageId()),
                        metadata.offset()));
            }
            return events;
        } catch (Exception e) {
            log.error("Could not inject fencing events! ", e);
            throw e;
        } finally {
           if (producer != null) {
               producer.close();
           }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(FenceEventInfo.listToJson(runMain(args)));
    }

    public static List<FenceEventInfo> runMain(String[] args) throws Exception {
        final  ArgumentParser parser = TierPartitionStateFencingTrigger.createArgParser();
        try {
            return run(parser, parser.parseArgs(args));
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            throw e;
        }
    }
}
