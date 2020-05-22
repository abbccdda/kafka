package kafka.tier.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import kafka.server.Defaults;
import kafka.tier.TopicIdPartition;
import kafka.tier.client.TierTopicClient;
import kafka.tier.client.TierTopicProducerSupplier;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.topic.TierTopic;
import kafka.tier.topic.TierTopicManagerConfig;
import kafka.tier.topic.TierTopicPartitioner;
import kafka.utils.CoreUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a set of static functions for recovery related tooling.
 */
public class RecoveryUtils {
    private static final Logger log = LoggerFactory.getLogger(RecoveryUtils.class);

    public static final String TIER_PROPERTIES_CONF_FILE_CONFIG = "tier.config";
    public static final String TIER_PROPERTIES_CONF_FILE_DOC =
        "The path to a configuration file containing the required properties";

    public static final String COMPARISON_TOOL_INPUT = "input.json";
    public static final String COMPARISON_TOOL_INPUT_DOC =
            "The path to a json file to be accepted as the input to the tool";

    public static final String COMPARISON_TOOL_OUTPUT = "output.json";
    public static final String COMPARISON_TOOL_OUTPUT_DOC =
            "The path to a json file where the tool will generate the output";

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
    public static short getNumPartitions(String bootstrapServers, String topicName)
        throws InterruptedException, ExecutionException {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, RecoveryUtils.class.getName());
        final Admin adminClient = Admin.create(props);

        final DescribeTopicsResult result =
            adminClient.describeTopics(Collections.singletonList(topicName));
        final Map<String, TopicDescription> descriptions;
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
     * Create and return a new TierTopic Producer object.
     *
     * @param bootstrapServers         the comma-separated list of bootstrap servers to be used by
     *                                 the producer (the string passed should be non-empty and
     *                                 valid)
     * @param tierTopicNamespace       the TierTopic namespace
     * @param numTierTopicPartitions   the number of partitions in the TierTopic
     * @param clientId                 the client ID to be used to construct the producer
     *
     * @return                         a newly created TierTopic producer object
     */
    public static Producer<byte[], byte[]> createTierTopicProducer(
        String bootstrapServers,
        String tierTopicNamespace,
        short numTierTopicPartitions,
        String clientId
    ) {
        final TierTopicManagerConfig config = new TierTopicManagerConfig(
            (Supplier<Map<String, Object>>) () -> Collections.singletonMap(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
            tierTopicNamespace,
            numTierTopicPartitions,
            Defaults.TierMetadataReplicationFactor(),
            -1,
            clientId,
            Defaults.TierMetadataMaxPollMs(),
            Defaults.TierMetadataRequestTimeoutMs(),
            Defaults.TierPartitionStateCommitInterval(),
            Collections.emptyList());
        final String tierTopicClientId = TierTopicClient.clientIdPrefix(clientId);
        final Producer<byte[], byte[]> newProducer
            = new KafkaProducer<>(TierTopicProducerSupplier.properties(config, tierTopicClientId));
        log.info(
            "Created new TierTopic producer! bootstrapServers={}, tierTopicNamespace={}" +
            ", numTierTopicPartitions={}, tierTopicClientId={}, newProducer={}",
            bootstrapServers, tierTopicNamespace, numTierTopicPartitions, tierTopicClientId, newProducer);
        return newProducer;
    }

    /**
     * Injects an event into the TierTopic, using the provided TierTopic producer object.
     *
     * @param producer                 the TierTopic producer object
     * @param event                    the event to be injected into the TierTopic
     * @param tierTopicName            the name of the TierTopic
     * @param numTierTopicPartitions   the number of TierTopic partitions
     *
     * @return                         the RecordMetadata obtained after successfully producing
     *                                 the event into the TierTopic
     *
     * @throws InterruptedException    if there was an error in producing the event
     * @throws ExecutionException      if there was an error in producing the event
     */
    public static RecordMetadata injectTierTopicEvent(
        Producer<byte[], byte[]> producer,
        AbstractTierMetadata event,
        String tierTopicName,
        short numTierTopicPartitions
    ) throws InterruptedException, ExecutionException {
        final TierTopicPartitioner partitioner = new TierTopicPartitioner(numTierTopicPartitions);
        final TopicPartition tierTopicPartition = TierTopic.toTierTopicPartition(
            event.topicIdPartition(), tierTopicName, partitioner);
        try {
            log.info(
                "Injecting TierTopic event: event={}, tierTopicPartition={}, tierTopicName={}" +
                ", numTierTopicPartitions={}",
                event, tierTopicPartition, tierTopicName, numTierTopicPartitions);
            final RecordMetadata injected = producer.send(
                new ProducerRecord<>(tierTopicPartition.topic(),
                    tierTopicPartition.partition(),
                    event.serializeKey(),
                    event.serializeValue())).get();
            log.info("Injected TierTopic event! recordMetadata={}", injected);
            return injected;
        } catch (InterruptedException | ExecutionException e) {
            log.error(
                "Failed to inject TierTopic event={}, tierTopicPartition={}, tierTopicName={}" +
                ", numTierTopicPartitions={}",
                event, tierTopicPartition, tierTopicName, numTierTopicPartitions, e);
            throw e;
        }
    }

    /**
     * Converts a list of formatted TopicIdPartition strings to a list of TopicIdPartition.
     * Each item in the input list should be a CSV string with the following format:
     * '<tiered_partition_topic_ID_base64_encoded>, <tiered_partition_topic_name>, <tiered_partition_name>'.
     *
     * @param topicIdPartitionsStr   the list of formatted TopicIdPartition strings
     *
     * @return                       a list of TopicIdPartition
     */
    public static List<TopicIdPartition> toTopicIdPartitions(List<String> topicIdPartitionsStr) {
        final List<TopicIdPartition> partitions = new ArrayList<>();
        for (String topicIdPartitionStr : topicIdPartitionsStr) {
            final String[] components = topicIdPartitionStr.split(",");
            if (components.length != 3) {
                throw new IllegalArgumentException(
                    String.format("'%s' does not contain 3 items.", topicIdPartitionStr));
            }

            final UUID topicId;
            try {
                topicId = CoreUtils.uuidFromBase64(components[0].trim());
            } catch (Exception e) {
                String msg = String.format(
                    "Item: '%s' has an invalid UUID provided as topic ID: '%s'", topicIdPartitionStr, components[0]);
                throw new IllegalArgumentException(msg, e);
            }

            final String topicName = components[1].trim();
            if (topicName.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(
                        "Item: '%s' cannot contain an empty topic name: '%s'",
                        topicIdPartitionStr, components[1]));
            }

            final int partition;
            try {
                partition = Integer.parseInt(components[2].trim());
            } catch (NumberFormatException e) {
                String msg = String.format(
                    "Item: '%s' has an illegal partition number: '%s'", topicIdPartitionStr, components[2]);
                throw new IllegalArgumentException(msg, e);
            }
            if (partition < 0) {
                throw new IllegalArgumentException(String.format(
                    "Item: '%s' cannot have a negative partition number: '%d'",
                    topicIdPartitionStr, partition));
            }
            partitions.add(new TopicIdPartition(topicName, topicId, partition));
        }

        return partitions;
    }

    public static String makeArgument(String arg) {
        return String.format("--%s", arg);
    }
}
