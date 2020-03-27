// (Copyright) [2020 - 2020] Confluent, Inc.
package org.apache.kafka.trogdor.workload.partitioner;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

public class GaussianPartitionerTest {
    private final static Node[] NODES = new Node[] {
        new Node(0, "localhost", 99),
        new Node(1, "localhost", 100),
        new Node(12, "localhost", 101)
    };

    private final static String TOPIC = "test";

    private final static List<PartitionInfo> PARTITIONS = asList(
        new PartitionInfo(TOPIC, 1, null, NODES, NODES),
        new PartitionInfo(TOPIC, 2, NODES[1], NODES, NODES),
        new PartitionInfo(TOPIC, 3, NODES[1], NODES, NODES),
        new PartitionInfo(TOPIC, 4, NODES[0], NODES, NODES),
        new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES));

    private final static Cluster CLUSTER = new Cluster("clusterId", asList(NODES),
        PARTITIONS, Collections.<String>emptySet(), Collections.<String>emptySet());

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testPartitioner() {
        for (int mean = 0; mean < PARTITIONS.size(); mean++) {
            testPartitionerWithMean(mean, 1);
        }

    }

    public void testPartitionerWithMean(int mean, int std) {
        Map<String, Integer> configs = new HashMap<>();
        configs.put(GaussianPartitionerConfig.MEAN_CONFIG, mean);
        configs.put(GaussianPartitionerConfig.STD_CONFIG, std);

        Partitioner partitioner = new GaussianPartitioner();
        partitioner.configure(configs);

        int[] distributions = new int[PARTITIONS.size()];

        for (int i = 0; i < 10000; i++) {
            int partition = partitioner.partition(TOPIC, null, null, null, null, CLUSTER);
            assertTrue(partition >= 0);
            assertTrue(partition < PARTITIONS.size());
            distributions[partition] += 1;
        }

        // basic assertions
        assertTrue(distributions[index(mean)] > distributions[index(mean - 1)]);
        assertTrue(distributions[index(mean - 1)] > distributions[index(mean - 2)]);

        assertTrue(distributions[index(mean)] > distributions[index(mean + 1)]);
        assertTrue(distributions[index(mean + 1)] > distributions[index(mean + 2)]);
    }

    private int index(int pos) {
        return (pos + PARTITIONS.size()) % PARTITIONS.size();
    }
}
