/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools;

import kafka.log.Log;
import kafka.tier.state.FileTierPartitionIterator;
import kafka.tier.state.FileTierPartitionState;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;

public class DumpTierPartitionState {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: path to partition data, e.g. "
                    + "/var/lib/kafka/log/mytopic-partitionNum");
            System.exit(1);
        }

        final File dir = new File(args[0]);
        final TopicPartition topicPartition = Log.parseTopicPartitionName(dir);
        System.out.println("Reading tier partition state for: " + topicPartition);
        try (FileTierPartitionState state = new FileTierPartitionState(dir, topicPartition,
                true,
                true)) {
            FileTierPartitionIterator iterator = state.iterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }
    }
}
