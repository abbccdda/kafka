// (Copyright) [2020 - 2020] Confluent, Inc.
package org.apache.kafka.trogdor.workload.partitioner;

import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

/**
 * GaussianPartitioner is a custom partition that distributes records following a
 * gaussian distribution. The keys of the records are ignored.
 */
public class GaussianPartitioner implements Partitioner {
    private final Random random = new Random();

    private int mean;
    private int std;

    @Override
    public void configure(final Map<String, ?> configs) {
        GaussianPartitionerConfig config = new GaussianPartitionerConfig(configs);
        mean = config.getInt(GaussianPartitionerConfig.MEAN_CONFIG);
        std = config.getInt(GaussianPartitionerConfig.STD_CONFIG);
    }

    @Override
    public int partition(final String topic, final Object key, final byte[] keyBytes,
            final Object value, final byte[] valueBytes, final Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        return nextPartition(partitions.size());
    }

    private int nextPartition(int partitionCount) {
        int partition;
        do {
            double val = random.nextGaussian() * std + mean;
            partition = ((int) Math.round(val) + partitionCount) % partitionCount;
        } while (partition < 0 || partition > partitionCount - 1);
        return partition;
    }

    @Override
    public void close() { }
}
