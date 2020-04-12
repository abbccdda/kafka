/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import kafka.server.Defaults;
import kafka.tier.TopicIdPartition;
import kafka.tier.client.TierTopicProducerSupplier;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierSegmentDeleteInitiate;
import kafka.tier.topic.TierTopicManagerConfig;
import kafka.tier.topic.TierTopicPartitioner;
import kafka.tier.topic.TierTopic;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool that injects a segment delete initiate event (generated at an epoch value of
 * int32 maximum) into TierTopic. This is helpful in triggering fencing of broker's
 * TierPartitionState materializer.
 */
public class TierPartitionStateFencingTrigger {
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap-servers";
    public static final String BOOTSTRAP_SERVERS_DOC =
        "List of comma-separated broker server and port string each in the form host:port";

    public static final String TIER_TOPIC_NAMESPACE_CONFIG = "tier-topic-namespace";
    public static final String TIER_TOPIC_NAMESPACE_DOC = "The tier topic namespace";

    public static final String DANGEROUS_FENCE_VIA_DELETE_EVENT_CMD = "dangerous-fence-via-delete-event";
    public static final String DANGEROUS_FENCE_VIA_DELETE_EVENT_DOC =
        "Triggers fencing by injecting a TierSegmentDeleteInitiate event with epoch set to int32 maximum: 2147483647.";

    public static final String TIERED_PARTITION_NAME_CONFIG = "tiered-partition-name";
    public static final String TIERED_PARTITION_NAME_DOC = "The name of the tiered partition";

    public static final String TIERED_PARTITION_TOPIC_NAME_CONFIG = "tiered-partition-topic-name";
    public static final String TIERED_PARTITION_TOPIC_NAME_DOC = "The name of the tiered partition topic";

    public static final String TIERED_PARTITION_TOPIC_ID_CONFIG = "tiered-partition-topic-id";
    public static final String TIERED_PARTITION_TOPIC_ID_DOC = "The UUID of the tiered partition topic";

    public static final String SELECTED_COMMAND = "selected_command";

    private static final Logger log = LoggerFactory.getLogger(TierPartitionStateFencingTrigger.class);

    private static String makeArument(String arg) {
        return String.format("--%s", arg);
    }

    // Create the CLI argument parser.
    private static ArgumentParser createArgParser() {
        ArgumentParser rootParser = ArgumentParsers
            .newArgumentParser(TierPartitionStateFencingTrigger.class.getName())
            .defaultHelp(true)
            .description("Provides commands to fence TierTopic in certain ways.");
        rootParser.addArgument(makeArument(TierPartitionStateFencingTrigger.BOOTSTRAP_SERVERS_CONFIG))
            .dest(TierPartitionStateFencingTrigger.BOOTSTRAP_SERVERS_CONFIG)
            .type(String.class)
            .required(true)
            .help(TierPartitionStateFencingTrigger.BOOTSTRAP_SERVERS_DOC);
        rootParser.addArgument(makeArument(TierPartitionStateFencingTrigger.TIER_TOPIC_NAMESPACE_CONFIG))
            .dest(TierPartitionStateFencingTrigger.TIER_TOPIC_NAMESPACE_CONFIG)
            .type(String.class)
            .required(false)
            .setDefault("")
            .help(TierPartitionStateFencingTrigger.TIER_TOPIC_NAMESPACE_DOC);
        rootParser.addArgument(makeArument(TierPartitionStateFencingTrigger.TIERED_PARTITION_NAME_CONFIG))
            .dest(TierPartitionStateFencingTrigger.TIERED_PARTITION_NAME_CONFIG)
            .type(Integer.class)
            .required(true)
            .help(TierPartitionStateFencingTrigger.TIERED_PARTITION_NAME_DOC);
        rootParser.addArgument(makeArument(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_NAME_CONFIG))
            .dest(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_NAME_CONFIG)
            .type(String.class)
            .required(true)
            .help(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_NAME_DOC);
        rootParser.addArgument(makeArument(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_ID_CONFIG))
            .dest(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_ID_CONFIG)
            .type(String.class)
            .required(true)
            .help(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_ID_DOC);

        Subparsers subparsers =
            rootParser.addSubparsers()
                .title("Valid subcommands")
                .dest(TierPartitionStateFencingTrigger.SELECTED_COMMAND);
        subparsers.addParser(TierPartitionStateFencingTrigger.DANGEROUS_FENCE_VIA_DELETE_EVENT_CMD)
                .help(TierPartitionStateFencingTrigger.DANGEROUS_FENCE_VIA_DELETE_EVENT_DOC);
        return rootParser;
    }

    // Main entry point for the CLI tool. This picks the sub-command logic to run. This may be
    // augmented in the future, with more sub-commands (if needed).
    private static void run(Namespace args) throws InterruptedException, ExecutionException {
        String selectedCommand = args.getString(TierPartitionStateFencingTrigger.SELECTED_COMMAND);
        switch (selectedCommand) {
            case DANGEROUS_FENCE_VIA_DELETE_EVENT_CMD:
                String tierTopicNamespace =
                    args.getString(TierPartitionStateFencingTrigger.TIER_TOPIC_NAMESPACE_CONFIG);
                // When injected, this event is expected to cause fencing, because of the very
                // high epoch value that inject into the event.
                AbstractTierMetadata eventThatTriggersFencing =
                    getDeleteInitiateEvent(args, Integer.MAX_VALUE);
                String bootstrapServers =
                    args.getString(TierPartitionStateFencingTrigger.BOOTSTRAP_SERVERS_CONFIG);
                injectTierTopicEvent(eventThatTriggersFencing, bootstrapServers, tierTopicNamespace);
                break;
            default:
                throw new IllegalStateException("Unsupported command: " + selectedCommand);
        }
    }

    /**
     * Discovers and returns the number of partitions in the provided topicName.
     * The implementation uses an Admin client to achieve the above.
     *
     * @param bootstrapServers   the comma-separated list of bootstrap servers to be used by the
     *                           admin client (the string passed should be non-empty and valid)
     * @param topicName          the name of the topic (should be non-empty and valid)
     *
     * @return   the number of partitions in the provided topicName.
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static short getNumTierTopicPartitions(String bootstrapServers, String topicName)
        throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, TierPartitionStateFencingTrigger.class.getName());
        Admin adminClient = Admin.create(props);

        DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));
        Map<String, TopicDescription> descriptions = null;
        try {
            descriptions = result.all().get();
        } finally {
            adminClient.close();
        }
        final int numPartitionsInt = descriptions.get(topicName).partitions().size();
        final short numPartitions = (short) numPartitionsInt;
        // Sanity check before casting.
        if (numPartitions != numPartitionsInt) {
            throw new IllegalStateException(
                String.format("Unexpected num partitions: %d", numPartitionsInt));
        }
        return numPartitions;
    }

    /**
     * Generates a TierSegmentDeleteInitiate event, with the epoch value set to provided value.
     * The partition number, topic name and the topic ID are extracted from the provided args.
     *
     * @param args    the CLI args useful to extract few required attributes
     * @param epoch   the epoch to be injected into the generated TierSegmentDeleteInitiate event
     * @return        the generated TierSegmentDeleteInitiate event
     */
    private static TierSegmentDeleteInitiate getDeleteInitiateEvent(Namespace args, int epoch) {
        String tieredPartitionTopicName =
            args.getString(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_NAME_CONFIG);
        UUID tieredPartitionTopicId =
            UUID.fromString(args.getString(TierPartitionStateFencingTrigger.TIERED_PARTITION_TOPIC_ID_CONFIG));
        int tieredPartitionInt =
            args.getInt(TierPartitionStateFencingTrigger.TIERED_PARTITION_NAME_CONFIG);
        TopicIdPartition tieredPartition = new TopicIdPartition(
            tieredPartitionTopicName, tieredPartitionTopicId, tieredPartitionInt);
        return new TierSegmentDeleteInitiate(
            tieredPartition, epoch, UUID.randomUUID());
    }

    /**
     * Injects an event into the TierTopic, using a newly created TierTopic producer object.
     *
     * @param event                the event to be injected into the TierTopic
     * @param bootstrapServers     the comma-separated list of bootstrap servers to be used by the
     *                             producer (the string passed should be non-empty and valid)
     * @param tierTopicNamespace   the TierTopic namespace (can be empty string if absent)
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private static void injectTierTopicEvent(
        AbstractTierMetadata event, String bootstrapServers, String tierTopicNamespace
    ) throws InterruptedException, ExecutionException {
        String tierTopicName = TierTopic.topicName(tierTopicNamespace);
        short numTierTopicPartitions = getNumTierTopicPartitions(bootstrapServers, tierTopicName);
        TierTopicPartitioner partitioner = new TierTopicPartitioner(numTierTopicPartitions);
        TopicPartition tierTopicPartition = TierTopic.toTierTopicPartition(
            event.topicIdPartition(), tierTopicName, partitioner);
        log.info(
            "Injecting TierTopic event: {} into TierTopic partition: {}", event, tierTopicPartition);

        TierTopicManagerConfig config = new TierTopicManagerConfig(
            new Supplier<Map<String, Object>>() {
                @Override
                public Map<String, Object> get() {
                    return Collections.singletonMap(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                }
            },
            tierTopicNamespace,
            numTierTopicPartitions,
            Defaults.TierMetadataReplicationFactor(),
            -1,
            "unknown",
            Defaults.TierMetadataMaxPollMs(),
            Defaults.TierMetadataRequestTimeoutMs(),
            Defaults.TierPartitionStateCommitInterval(),
            Collections.emptyList());
        Producer<byte[], byte[]> producer = null;
        try {
            producer = new TierTopicProducerSupplier(config).get();
            RecordMetadata injected = producer.send(
                new ProducerRecord<>(tierTopicPartition.topic(),
                    tierTopicPartition.partition(),
                    event.serializeKey(),
                    event.serializeValue())).get();
            log.info("Injected TierTopic event! RecordMetadata: {}", injected);
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = TierPartitionStateFencingTrigger.createArgParser();
        Namespace parsedArgs = null;
        try {
            parsedArgs = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        run(parsedArgs);
    }
}
