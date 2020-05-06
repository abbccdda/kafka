/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;

public class ExecutorAdminUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorAdminUtils.class);

  private ConfluentAdmin adminClient;
  private final KafkaCruiseControlConfig config;

  public ExecutorAdminUtils(ConfluentAdmin adminClient, KafkaCruiseControlConfig config) {
    this.adminClient = adminClient;
    this.config = config;
  }

  /**
   * Returns the replicas for a given partition. If the topic or partition doesn't exist, return an empty collection
   *
   * @param topicPartition the partition to fetch the replicas for
   */
  public List<Integer> getReplicasForPartition(TopicPartition topicPartition) {
    try {
      Map<String, TopicDescription> descriptions = adminClient.describeTopics(Collections.singletonList(topicPartition.topic()))
                                                              .all()
                                                              .get(config.getLong(DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
      TopicDescription topicDesc = descriptions.get(topicPartition.topic());
      if (topicDesc == null) {
        return Collections.emptyList();
      }

      Optional<TopicPartitionInfo> partitionInfoOpt = topicDesc.partitions().stream()
                                                               .filter(tp -> tp.partition() == topicPartition.partition())
                                                               .findAny();
      if (partitionInfoOpt.isPresent()) {
        return partitionInfoOpt.get().replicas().stream().map(Node::id).collect(Collectors.toList());
      }
    } catch (InterruptedException | ExecutionException | TimeoutException | KafkaException e) {
      LOG.warn("Encountered exception while fetching the replicas for topic partition {}", topicPartition, e);
    }

    return Collections.emptyList();
  }

  /**
   * Cancels any partition reassignments for the given topic partitions
   *
   * @return the number of in-progress partition reassignments that were cancelled
   */
  public int cancelInterBrokerReplicaMovements(List<TopicPartition> partitionReassignmentsToCancel) {
    int numCancelled = 0;
    Optional<NewPartitionReassignment> cancelReassignment = Optional.empty();
    Map<TopicPartition, Optional<NewPartitionReassignment>> partitionsToCancel =
        partitionReassignmentsToCancel.stream().collect(
            Collectors.toMap(pr -> pr, pr -> cancelReassignment)
        );

    Map<TopicPartition, KafkaFuture<Void>> cancellationFutures =
        adminClient.alterPartitionReassignments(partitionsToCancel).values();

    for (Map.Entry<TopicPartition, KafkaFuture<Void>> futureEntry : cancellationFutures.entrySet()) {
      TopicPartition tp = futureEntry.getKey();
      try {
        futureEntry.getValue().get();
        numCancelled += 1;
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while cancelling partition reassignments.");
        break;
      } catch (ExecutionException | ApiException e) {
        if (e.getCause() instanceof NoReassignmentInProgressException || e instanceof NoReassignmentInProgressException) {
          LOG.debug("Could not cancel reassignment of {} because none was in progress", tp);
        } else if (e.getCause() instanceof UnsupportedVersionException) {
          LOG.warn("Kafka does not support the AlterPartitionReassignments API." +
                  "Cannot cancel the current partition reassignments.");
          break;
        } else {
          LOG.warn("Reassignment cancellation for {} failed.", tp, e);
        }
      }
    }
    return numCancelled;
  }

  /**
   * Fetch the logdir information for subject replicas in intra-broker replica movement tasks.
   *
   * @param tasks The tasks to check.
   * @return Replica logdir information by task.
   */
  public Map<ExecutionTask, ReplicaLogDirInfo> getLogdirInfoForExecutionTask(Collection<ExecutionTask> tasks) {
    Set<TopicPartitionReplica> replicasToCheck = new HashSet<>(tasks.size());
    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask = new HashMap<>(tasks.size());
    Map<TopicPartitionReplica, ExecutionTask> taskByReplica = new HashMap<>(tasks.size());
    tasks.forEach(t -> {
      TopicPartitionReplica tpr = new TopicPartitionReplica(t.proposal().topic(), t.proposal().partitionId(), t.brokerId());
      replicasToCheck.add(tpr);
      taskByReplica.put(tpr, t);
    });
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> logDirsByReplicas = adminClient.describeReplicaLogDirs(replicasToCheck).values();
    for (Map.Entry<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> entry : logDirsByReplicas.entrySet()) {
      try {
        ReplicaLogDirInfo info = entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
        logdirInfoByTask.put(taskByReplica.get(entry.getKey()), info);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.warn("Encounter exception {} when fetching logdir information for replica {}", e.getMessage(), entry.getKey());
      }
    }
    return logdirInfoByTask;
  }

  /**
   * Execute intra-broker replica movement tasks by sending alterReplicaLogDirs request.
   *
   * @param tasksToExecute The tasks to execute.
   * @param executionTaskManager The task manager to do bookkeeping for task execution state.
   */
  public void executeIntraBrokerReplicaMovements(List<ExecutionTask> tasksToExecute,
                                                 ExecutionTaskManager executionTaskManager) {
    Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>(tasksToExecute.size());
    Map<TopicPartitionReplica, ExecutionTask> replicaToTask = new HashMap<>(tasksToExecute.size());
    tasksToExecute.forEach(t -> {
      TopicPartitionReplica tpr = new TopicPartitionReplica(t.proposal().topic(), t.proposal().partitionId(), t.brokerId());
      replicaAssignment.put(tpr, t.proposal().replicasToMoveBetweenDisksByBroker().get(t.brokerId()).logdir());
      replicaToTask.put(tpr, t);
    });
    for (Map.Entry<TopicPartitionReplica, KafkaFuture<Void>> entry: adminClient.alterReplicaLogDirs(replicaAssignment).values().entrySet()) {
      try {
        entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException |
               LogDirNotFoundException | KafkaStorageException | ReplicaNotAvailableException e) {
        LOG.warn("Encounter exception {} when trying to execute task {}, mark task dead.", e.getMessage(), replicaToTask.get(entry.getKey()));
        executionTaskManager.markTaskAborting(replicaToTask.get(entry.getKey()));
        executionTaskManager.markTaskDead(replicaToTask.get(entry.getKey()));
      }
    }
  }

  /**
   * Check whether there is ongoing intra-broker replica movement.
   * @param brokersToCheck List of broker to check.
   * @return True if there is ongoing intra-broker replica movement.
   */
  public boolean isOngoingIntraBrokerReplicaMovement(Collection<Integer> brokersToCheck) {
    Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> logDirsByBrokerId = adminClient.describeLogDirs(brokersToCheck).values();
    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirInfo>>> entry : logDirsByBrokerId.entrySet()) {
      try {
        Map<String, LogDirInfo> logInfos = entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
        for (LogDirInfo info : logInfos.values()) {
          if (info.error == Errors.NONE) {
            if (info.replicaInfos.values().stream().anyMatch(i -> i.isFuture)) {
              return true;
            }
          }
        }
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        //Let it go.
      }
    }
    return false;
  }
}

