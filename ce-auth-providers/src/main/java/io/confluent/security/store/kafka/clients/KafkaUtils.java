// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {
  private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

  private static final int RETRY_BACKOFF_MS = 10;

  /**
   * Waits for full partition metadata for topic with `numPartitions` partitions to be available.
   * This method handles edge cases where partial metadata for a topic may be available on the broker.
   *
   *
   * @param topic Name of topic to wait for
   * @param numPartitions Expected number of partitions
   * @param time Instance of time used for timeout
   * @param timeout Timeout duration
   * @param describeTopic Topic describe function that returns set of partitions returned
   *                      from a metadata request. Returned set may be empty.
   * @param createTopic Function used to create topic. If null, topic is not created and
   *                    this method just waits for topic to be available.
   *
   * @throws IllegalStateException if the actual number of partitions exceeds `numPartitions`
   * @throws TimeoutException if partition metadata for all of `numPartitions` partitions are
   *         not available within timeout
   * @throws KafkaException if describe or create fails with an non-retriable exception
   */
  public static void waitForTopic(String topic,
                                  int numPartitions,
                                  Time time,
                                  Duration timeout,
                                  Function<String, Set<Integer>> describeTopic,
                                  Consumer<String> createTopic) {
    long timeoutMs = timeout.toMillis();
    long endTimeMs = time.milliseconds() + timeout.toMillis();
    Set<Integer> expectedPartitions = IntStream.range(0, numPartitions)
        .boxed().collect(Collectors.toSet());
    Set<Integer> partitions = Collections.emptySet();
    boolean created = false;
    while (true) {
      RetriableException describeException = null;
      ApiException createException = null;
      try {
        partitions = describeTopic.apply(topic);
      } catch (UnknownTopicOrPartitionException e) {
        if (!created && createTopic != null) {
          try {
            log.debug("Topic not found, attempting to create topic {}", topic);
            createTopic.accept(topic);
            created = true;
          } catch (RetriableException | InvalidReplicationFactorException e1) {
            log.debug("Failed to create topic " +  topic, e1);
            createException = e1;
          }
        }
      } catch (RetriableException e) {
        log.debug("Partition info could not be obtained for " + topic, e);
        describeException = e;
      }

      if (partitions != null && !partitions.isEmpty()) {
        if (expectedPartitions.equals(partitions)) {
          log.debug("Topic {} has the expected {} partitions, returning", topic, numPartitions);
          return;
        } else if (partitions.size() >= numPartitions || Collections.max(partitions) > numPartitions - 1) {
          throw new IllegalStateException(String.format("Unexpected partitions for topic %s: expected 0-%d, got %s",
              topic, numPartitions - 1, partitions));
        } else
          log.debug("Topic {} has partitions {}, waiting for 0-{}", topic, partitions, numPartitions - 1);
      }

      long remainingMs = endTimeMs - time.milliseconds();
      if (remainingMs <= 0) {
        ApiException cause = createException != null ? createException : describeException;
        throw new TimeoutException(String.format("Full metadata for topic %s not available within timeout %s ms, " +
            "available partitions %s, expected 0-%d", topic, timeoutMs, partitions, numPartitions - 1), cause);
      } else {
        time.sleep(Math.min(remainingMs, RETRY_BACKOFF_MS));
      }
    }
  }
}
