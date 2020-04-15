package kafka.tier.tools;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import kafka.server.Defaults;
import kafka.tier.client.TierTopicProducerSupplier;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.topic.TierTopic;
import kafka.tier.topic.TierTopicManagerConfig;
import kafka.tier.topic.TierTopicPartitioner;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a set of static functions for recovery related tooling.
 */
public class RecoveryUtils {

    // Defines common CLI option names and documentation.
    public interface CommonCLIOptions {
        String BOOTSTRAP_SERVERS_CONFIG = "bootstrap-servers";
        String BOOTSTRAP_SERVERS_DOC =
            "List of comma-separated broker server and port string each in the form host:port";

        String TIER_TOPIC_NAMESPACE_CONFIG = "tier-topic-namespace";
        String TIER_TOPIC_NAMESPACE_DOC = "The tier topic namespace";

        String TIERED_PARTITION_NAME_CONFIG = "tiered-partition-name";
        String TIERED_PARTITION_NAME_DOC = "The name of the tiered partition";

        String TIERED_PARTITION_TOPIC_NAME_CONFIG = "tiered-partition-topic-name";
        String TIERED_PARTITION_TOPIC_NAME_DOC = "The name of the tiered partition topic";

        String TIERED_PARTITION_TOPIC_ID_CONFIG = "tiered-partition-topic-id";
        String TIERED_PARTITION_TOPIC_ID_DOC = "The UUID of the tiered partition topic";
    }

    private static final Logger log = LoggerFactory.getLogger(RecoveryUtils.class);

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
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, RecoveryUtils.class.getName());
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
     * Injects an event into the TierTopic, using a newly created TierTopic producer object.
     *
     * @param event                the event to be injected into the TierTopic
     * @param bootstrapServers     the comma-separated list of bootstrap servers to be used by the
     *                             producer (the string passed should be non-empty and valid)
     * @param tierTopicNamespace   the TierTopic namespace (can be empty string if absent)
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void injectTierTopicEvent(
        AbstractTierMetadata event, String bootstrapServers, String tierTopicNamespace, String clusterId
    ) throws InterruptedException, ExecutionException {
        String tierTopicName = TierTopic.topicName(tierTopicNamespace);
        short numTierTopicPartitions = RecoveryUtils.getNumPartitions(bootstrapServers, tierTopicName);
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
            clusterId,
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

    // Populates common useful CLI options into provider parser.
    public static void populateCommonCLIOptions(ArgumentParser parser) {
        parser.addArgument(makeArgument(CommonCLIOptions.BOOTSTRAP_SERVERS_CONFIG))
            .dest(CommonCLIOptions.BOOTSTRAP_SERVERS_CONFIG)
            .type(String.class)
            .required(true)
            .help(CommonCLIOptions.BOOTSTRAP_SERVERS_DOC);
        parser.addArgument(makeArgument(CommonCLIOptions.TIER_TOPIC_NAMESPACE_CONFIG))
            .dest(CommonCLIOptions.TIER_TOPIC_NAMESPACE_CONFIG)
            .type(String.class)
            .required(false)
            .setDefault("")
            .help(CommonCLIOptions.TIER_TOPIC_NAMESPACE_DOC);
        parser.addArgument(makeArgument(CommonCLIOptions.TIERED_PARTITION_NAME_CONFIG))
            .dest(CommonCLIOptions.TIERED_PARTITION_NAME_CONFIG)
            .type(Integer.class)
            .required(true)
            .help(CommonCLIOptions.TIERED_PARTITION_NAME_DOC);
        parser.addArgument(makeArgument(CommonCLIOptions.TIERED_PARTITION_TOPIC_NAME_CONFIG))
            .dest(CommonCLIOptions.TIERED_PARTITION_TOPIC_NAME_CONFIG)
            .type(String.class)
            .required(true)
            .help(CommonCLIOptions.TIERED_PARTITION_TOPIC_NAME_DOC);
        parser.addArgument(makeArgument(CommonCLIOptions.TIERED_PARTITION_TOPIC_ID_CONFIG))
            .dest(CommonCLIOptions.TIERED_PARTITION_TOPIC_ID_CONFIG)
            .type(String.class)
            .required(true)
            .help(CommonCLIOptions.TIERED_PARTITION_TOPIC_ID_DOC);
    }

    private static String makeArgument(String arg) {
        return String.format("--%s", arg);
    }
}
