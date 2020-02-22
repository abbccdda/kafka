/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import kafka.log.Log;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.serdes.TierKafkaKey;
import kafka.tier.state.FileTierPartitionState;
import kafka.tier.state.TierPartitionState;
import kafka.tier.topic.TierMessageFormatter;
import kafka.tier.topic.TierTopicManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * TierTopicMaterializationUtils provides utils for dumping materialization events from tier topic for the given source
 * topic(or all) for a give source partition(or all). Additionally it can recompute the materialized state and dump it too.
 * This utils will be used by various tools for debugging, validation and recovering of tier states.
 */
public class TierTopicMaterializationUtils {
    private final String topic = Topic.TIER_TOPIC_NAME;
    public KafkaConsumer<byte[], byte[]> consumer;
    private ConsumerRecords<byte[], byte[]> records;
    public TierTopicMaterializationToolConfig config;
    private HashMap<TopicIdPartition, Long> offsetMap = null;
    public HashMap<TopicIdPartition, FileTierPartitionState> stateMap = new HashMap<>();
    private UserTierPartition targetTierPartition;
    private Set<TopicIdPartition> doneConsumption = null;

    // Keep dumping output of every 1000th event if configuration do not expe ct dumping of every event.
    // This is super useful for tracking long running jobs.
    private static final int SAMPLING_INTERVAL = 1000;
    // Number of Partitions in tier topic. This needs to be replaced with interface, not sure if there
    // exists any.

    public TierTopicMaterializationUtils(TierTopicMaterializationToolConfig config, HashMap<TopicIdPartition, Long> offsetMap) {
        this(config);
        this.offsetMap = offsetMap;
    }

    TierTopicMaterializationUtils(TierTopicMaterializationToolConfig config) {
        this.config = config;
        consumer = new KafkaConsumer(getConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        this.targetTierPartition = new UserTierPartition(null, config.userTopicId, config.userPartition);
        this.doneConsumption = new HashSet<>();
    }

    public void setupConsumer(TierTopicMaterializationToolConfig config) {
        // Init
        if (config.partition != -1) {
            seek(this.topic, config.partition, config.startOffset);
        } else {
            // Listen on all the partitions of tier topic.
            List<PartitionInfo> partitions = consumer.partitionsFor(this.topic);
            Set<TopicPartition> tierTopicPartitions = TierTopicManager.partitions(this.topic,
                partitions.size());

            System.out.println("Listening on all " + partitions.size() + " partitions of tier topic");
            consumer.assign(tierTopicPartitions);
            consumer.seekToBeginning(tierTopicPartitions);
        }
    }

    private void fetchRecords() {
        this.records = consumer.poll(Duration.ofSeconds(30));
        if (this.records.isEmpty()) {
            // Raise Timeout exception in general, also interpreted for end of loop.
            throw new TimeoutException();
        }
    }

    private File getStateFolder(TopicIdPartition id) {
        return new File(config.materializationPath + "/" + id.topic() + "-" + id.partition());
    }

    public void run() throws IOException {
        boolean materialize = this.config.getBoolean(TierTopicMaterializationToolConfig.MATERIALIZE);
        int recordCount = 0;
        int currentOffset = config.startOffset;
        TierMessageFormatter writer = new TierMessageFormatter();
        setupConsumer(config);

        System.out.println("Materializing from " + config.startOffset + " till  " + config.endOffset);
        try {
            while (config.endOffset == -1 || currentOffset <= config.endOffset) {
                fetchRecords();
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    // Note: assumption is endOffset is set only for fetch from a single partition. A check is in place at
                    // config layer. With this we keep assigning the offset to endOffset as current offset value.
                    currentOffset = (int) record.offset();
                    TopicIdPartition id = AbstractTierMetadata.deserializeKey(record.key());

                    // Fetch the topicIdPartition from the record and materialize the event if applicable.
                    if (doMaterialize(id, record.offset())) {
                        recordCount++;
                        // If it's not needed to dump all the events then dump samples to track long
                        // running tasks progress.
                        if (config.dumpEvents || recordCount % TierTopicMaterializationUtils.SAMPLING_INTERVAL == 0)
                            writer.writeTo(record, System.out);
                        if (materialize)
                            materializeRecord(record, id);
                    }
                }

                if (exitLoop()) {
                    System.out.println("Done reading events for all configured source topic partitions.");
                    break;
                } 
            }
        } catch (WakeupException | TimeoutException we) {
            // Output the exception and continue post processing. Most cases where the endOffset is not set this will be
            // the trigger for ending poll loop.
            System.out.println("Timeout after processing " + recordCount + " messages.");
        }

        saveMaterializedStates();
        dumpMaterializedState();
    }

    private boolean exitLoop() {
        // If offset map is not populated we will not exit consumer traversal until and unless end
        // offset or timeout happens.
        return offsetMap != null && offsetMap.isEmpty();
    }

    private void saveMaterializedStates() throws  IOException {
        for (TopicIdPartition id: stateMap.keySet()) {
            System.out.println("Closing state file " + id);
            stateMap.get(id).close();
        }
    }

    /**
     * Determines if an event from a given TopicIdPartition qualifies for materialization of further
     * processing. Make sure this check is done in constant order as it will be called on every
     * iteration. If offsetMap is populated, caller has already expressed list of TopicPartition for
     * which events will be collected/materialized. Otherwise, if caller has specified a given topic
     * and any partition then that will be filtered.
     * @param tpid TopicIdPartition
     * @param offset of the latest event from tier state topic partition.
     * @return true if given TopicIdPartition should process event at the given offset.
     */
    private boolean doMaterialize(TopicIdPartition tpid, Long offset) {
        if (this.offsetMap != null) {
            if (!this.offsetMap.containsKey(tpid)) {
                return false;
            } else if (this.offsetMap.get(tpid) >= offset) {
                return true;
            } else {
                this.offsetMap.remove(tpid);
            }
            return false;
        } else if (this.targetTierPartition.filter(tpid.topic(), tpid.partition(), tpid.topicId())) {
            // If user has configured only specific topic and any partition, targetTierPartition will
            // filter out rest of the TierPartitions. If not configured it will allow all.
            return true;
        }

        // We reached here meaning tier topic partition not configured for materialization.
        return false;
    }

    private void materializeRecord(ConsumerRecord<byte[], byte[]> record, TopicIdPartition id)
            throws IOException {
        FileTierPartitionState state;
        state = getStateIfRequired(record, id);
        final Optional<AbstractTierMetadata> entryOpt =
                AbstractTierMetadata.deserialize(record.key(), record.value());
        if (entryOpt.isPresent()) {
            AbstractTierMetadata entry = entryOpt.get();
            TierPartitionState.AppendResult result = state.append(entry, record.offset());
            if (!result.equals(TierPartitionState.AppendResult.ACCEPTED))
                System.out.println(state.append(entry, record.offset()) + " offset " + record.offset());
        }
    }

    private FileTierPartitionState getStateIfRequired(ConsumerRecord<byte[], byte[]> record, TopicIdPartition id)
        throws IOException {
        // Maintain a map of source topic partition and its states file.
        if (!stateMap.containsKey(id)) {
            TierKafkaKey tierKey = TierKafkaKey.getRootAsTierKafkaKey(ByteBuffer.wrap(record.key()));
            File path = getStateFolder(id);
            if (!path.exists()) path.mkdirs();
            FileTierPartitionState state = new FileTierPartitionState(path, new TopicPartition(tierKey.topicName(), tierKey.partition()), true);
            state.setTopicId(new UUID(tierKey.topicId().mostSignificantBits(),
                    tierKey.topicId().leastSignificantBits()));
            stateMap.put(id, state);
            // The state should be in ONLINE state.
            stateMap.get(id).onCatchUpComplete();
        }
        return stateMap.get(id);
    }

    private void dumpMaterializedState() {
        if (!config.dumpRecords && !config.dumpHeader)
            return;

        Iterator<TopicIdPartition> it = stateMap.keySet().iterator();
        while (it.hasNext()) {
            TopicIdPartition id = it.next();
            File basePath = getStateFolder(id);
            for (File file : basePath.listFiles()) {
                if (file.isFile() && Log.isTierStateFile(file)) {
                    System.out.println("Dumping for " + basePath);
                    if (config.dumpHeader)
                        DumpTierPartitionState.dumpTierState(id.topicPartition(), file, true);
                    else
                        DumpTierPartitionState.dumpTierState(id.topicPartition(), file, false);
                }
            }
        }
    }

    public Path getTierStateFile(TopicIdPartition id) {
        File basePath = getStateFolder(id);
        for (File file : basePath.listFiles()) {
            if (file.isFile() && Log.isTierStateFile(file)) {
                return file.toPath();
            }
        }
        return null;
    }

    private void seek(String topic, Integer partition, Integer offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, offset);
    }

    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.server);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tier-topic-materialization-tool");
        return props;
    }

    private class UserTierPartition {
        final String name;
        final UUID id;
        final Integer partitionId;

        UserTierPartition(String name, UUID id, Integer partitionId) {
            this.name = name;
            this.id = id;
            this.partitionId = partitionId;
        }

        public boolean filter(String name, Integer partitonId, UUID id) {
            return (this.name == null || this.name.equals(name)) &&
                    (this.id.equals(TierTopicMaterializationToolConfig.EMPTY_UUID) || this.id.equals(id)) &&
                    (this.partitionId.equals(-1) || this.partitionId.equals(partitonId));
        }
    }
}
