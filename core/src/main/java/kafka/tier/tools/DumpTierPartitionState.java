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
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public class DumpTierPartitionState {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: path to partition data, e.g. /var/lib/kafka/log/mytopic-partitionNum");
            System.exit(1);
        }

        final File dir = new File(args[0]);
        final TopicPartition topicPartition = Log.parseTopicPartitionName(dir);

        System.out.println("Reading tier partition state for " + topicPartition);
        for (File file : dir.listFiles()) {
            if (file.isFile() && Log.isTierStateFile(file))
                dumpTierState(topicPartition, file);
        }
    }

    private static void dumpTierState(TopicPartition topicPartition, File file) {
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            System.out.println("Dumping state in file " + file);
            Optional<FileTierPartitionIterator> iteratorOpt = FileTierPartitionState.iterator(topicPartition, fileChannel);
            if (!iteratorOpt.isPresent()) {
                System.out.println("Empty file");
                return;
            }

            while (iteratorOpt.get().hasNext())
                System.out.println(iteratorOpt.get().next());
        } catch (IOException e) {
            System.err.println("Caught exception for file " + file);
            e.printStackTrace();
        }
    }
}
