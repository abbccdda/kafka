package kafka.tier.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import kafka.server.Defaults;
import kafka.tier.TopicIdPartition;
import kafka.tier.client.TierTopicClient;
import kafka.tier.client.TierTopicProducerSupplier;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.topic.TierTopic;
import kafka.tier.topic.TierTopicPartitioner;
import kafka.utils.CoreUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
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
     * @param topicName          the name of the topic (should be non-empty and valid)
     *
     * @return   the number of partitions in the provided topicName.
     *
     */
    public static int getNumPartitions(Producer<byte[], byte[]> producer, String topicName) {
        List<PartitionInfo> partitions = producer.partitionsFor(topicName);
        Optional<Integer> max = partitions.stream().map(PartitionInfo::partition).max(Integer::compareTo);
        if (!max.isPresent())
            throw new IllegalStateException("Partitions not found for tier topic " + topicName);

        if (max.get() + 1 > partitions.size())
            throw new IllegalStateException("Partitions missing for tier topic " + topicName);

        return partitions.size();
    }

    /**
     * Create and return a new TierTopic Producer object.
     *
     * @param properties               properties object to pass to the producer
     * @param clientId                 the client ID to be used to construct the producer
     *
     * @return                         a newly created TierTopic producer object
     */
    public static Producer<byte[], byte[]> createTierTopicProducer(
        Properties properties,
        String clientId
    ) {
        final String tierTopicClientId = TierTopicClient.clientIdPrefix(clientId);
        Properties producerProperties = new Properties();
        producerProperties.putAll(properties);
        TierTopicProducerSupplier.addBaseProperties(producerProperties, tierTopicClientId,
                Defaults.TierMetadataRequestTimeoutMs());
        final Producer<byte[], byte[]> newProducer = new KafkaProducer<>(producerProperties);
        log.info(
            "Created new TierTopic producer! properties={}, " +
            ", tierTopicClientId={}, newProducer={}",
            properties, tierTopicClientId, newProducer);
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
        int numTierTopicPartitions
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
