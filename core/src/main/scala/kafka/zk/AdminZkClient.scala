/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.zk

import java.util.{Properties, UUID}

import kafka.admin.{AdminOperationException, AdminUtils, BrokerMetadata, RackAwareMode}
import kafka.cluster.Observer
import kafka.common.{TopicAlreadyMarkedForDeletionException, TopicPlacement}
import kafka.controller.ReplicaAssignment
import kafka.log.LogConfig
import kafka.server.{ConfigEntityName, ConfigType, DynamicConfig}
import kafka.server.link.ClusterLinkTopicState
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.collection.{Map, Seq}

/**
 * Provides admin related methods for interacting with ZooKeeper.
 *
 * This is an internal class and no compatibility guarantees are provided,
 * see org.apache.kafka.clients.admin.AdminClient for publicly supported APIs.
 */
class AdminZkClient(zkClient: KafkaZkClient) extends Logging {

  /**
   * Creates the topic with given configuration
   * @param topic topic name to create
   * @param partitions  Number of partitions to be set
   * @param replicationFactor Replication factor
   * @param topicConfig  topic configs
   * @param rackAwareMode rack-aware mode
   * @param createTopicId Boolean indicating if topic id should be created
   * @param clusterLink Optional cluster link topic state
   */
  def createTopic(topic: String,
                  partitions: Int,
                  replicationFactor: Int,
                  topicConfig: Properties = new Properties,
                  rackAwareMode: RackAwareMode = RackAwareMode.Enforced,
                  createTopicId: Boolean = false,
                  clusterLink: Option[ClusterLinkTopicState] = None): Unit = {
    val brokerMetadatas = getBrokerMetadatas(rackAwareMode)
    val replicaAssignment = AdminUtils
      .assignReplicasToBrokers(brokerMetadatas, partitions, replicationFactor)
      .map { case (partition, replicas) =>
        (partition, ReplicaAssignment(replicas, Seq.empty))
      }
    createTopicWithAssignment(topic, topicConfig, replicaAssignment, createTopicId, clusterLink)
  }

  /**
   * Gets broker metadata list
   * @param rackAwareMode
   * @param brokerList
   * @return
   */
  def getBrokerMetadatas(rackAwareMode: RackAwareMode = RackAwareMode.Enforced,
                         brokerList: Option[Seq[Int]] = None): Seq[BrokerMetadata] = {
    val allBrokers = zkClient.getAllBrokersInCluster
    val brokers = brokerList.map(brokerIds => allBrokers.filter(b => brokerIds.contains(b.id))).getOrElse(allBrokers)
    val brokersWithRack = brokers.filter(_.rack.nonEmpty)
    if (rackAwareMode == RackAwareMode.Enforced && brokersWithRack.nonEmpty && brokersWithRack.size < brokers.size) {
      throw new AdminOperationException("Not all brokers have rack information. Add --disable-rack-aware in command line" +
        " to make replica assignment without rack information.")
    }
    val brokerMetadatas = rackAwareMode match {
      case RackAwareMode.Disabled => brokers.map(broker => BrokerMetadata(broker.id, None))
      case RackAwareMode.Safe if brokersWithRack.size < brokers.size =>
        brokers.map(broker => BrokerMetadata(broker.id, None))
      case _ => brokers.map(broker => BrokerMetadata(broker.id, broker.rack))
    }
    brokerMetadatas.sortBy(_.id)
  }

  def createTopicWithAssignment(topic: String,
                                config: Properties,
                                partitionReplicaAssignment: Map[Int, ReplicaAssignment],
                                createTopicId: Boolean = false,
                                clusterLink: Option[ClusterLinkTopicState] = None): Unit = {
    validateTopicCreate(topic, partitionReplicaAssignment, config)

    info(s"Creating topic $topic with configuration $config and initial partition " +
      s"assignment $partitionReplicaAssignment")

    // write out the config if there is any, this isn't transactional with the partition assignments
    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic, config)

    // create the partition assignment
    writeTopicPartitionAssignment(topic, partitionReplicaAssignment, isUpdate = false,
      createTopicId = createTopicId, clusterLink)
  }

  /**
   * Validate topic creation parameters
   *
   * For observers, the observer assignment needs to be a subset of the
   * replica assignment.
   */
  def validateTopicCreate(topic: String,
                          partitionReplicaAssignment: Map[Int, ReplicaAssignment],
                          config: Properties): Unit = {
    Topic.validate(topic)

    if (zkClient.topicExists(topic))
      throw new TopicExistsException(s"Topic '$topic' already exists.")
    else if (Topic.hasCollisionChars(topic)) {
      val allTopics = zkClient.getAllTopicsInCluster()
      // check again in case the topic was created in the meantime, otherwise the
      // topic could potentially collide with itself
      if (allTopics.contains(topic))
        throw new TopicExistsException(s"Topic '$topic' already exists.")
      val collidingTopics = allTopics.filter(Topic.hasCollision(topic, _))
      if (collidingTopics.nonEmpty) {
        throw new InvalidTopicException(s"Topic '$topic' collides with existing topics: ${collidingTopics.mkString(", ")}")
      }
    }

    if (partitionReplicaAssignment.values.map(_.replicas.size).toSet.size != 1)
      throw new InvalidReplicaAssignmentException("All partitions should have the same number of replicas")

    if (partitionReplicaAssignment.values.map(_.observers.size).toSet.size != 1)
      throw new InvalidReplicaAssignmentException("All partitions should have the same number of observers")

    partitionReplicaAssignment.values.foreach { assignment =>
      if (assignment.replicas.size != assignment.replicas.toSet.size) {
        throw new InvalidReplicaAssignmentException(
          s"Duplicate replica assignment found: $partitionReplicaAssignment"
        )
      }

      if (assignment.observers.size != assignment.observers.toSet.size) {
        throw new InvalidReplicaAssignmentException(
          s"Duplicate observers assignment found: $partitionReplicaAssignment"
        )
      }
    }


    val partitionSize = partitionReplicaAssignment.size
    val sequenceSum = partitionSize * (partitionSize - 1) / 2
    if (partitionReplicaAssignment.size != partitionReplicaAssignment.toSet.size ||
        partitionReplicaAssignment.keys.filter(_ >= 0).sum != sequenceSum)
        throw new InvalidReplicaAssignmentException("partitions should be a consecutive 0-based integer sequence")

    LogConfig.validate(config)
  }

  // Visibility for tests
  def writeTopicPartitionAssignment(topic: String, replicaAssignment: Map[Int, ReplicaAssignment],
                                    isUpdate: Boolean, createTopicId: Boolean = false,
                                    clusterLink: Option[ClusterLinkTopicState] = None): Unit = {
    try {
      val assignment = replicaAssignment.map { case (partitionId, replicas) => (new TopicPartition(topic,partitionId), replicas) }.toMap

      if (!isUpdate) {
        val topicIdOpt = if (createTopicId)
          Some(UUID.randomUUID())
        else
          None

        zkClient.createTopicAssignment(topic, topicIdOpt, assignment, clusterLink)
      } else {
        val topicIds = zkClient.getTopicIdsForTopics(Set(topic))
        zkClient.setTopicAssignment(topic, topicIds.get(topic), assignment, clusterLink)
      }
      debug("Updated path %s with %s for replica assignment".format(TopicZNode.path(topic), assignment))
    } catch {
      case _: NodeExistsException => throw new TopicExistsException(s"Topic '$topic' already exists.")
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  /**
   * Creates a delete path for a given topic
   * @param topic
   */
  def deleteTopic(topic: String): Unit = {
    if (zkClient.topicExists(topic)) {
      try {
        zkClient.createDeleteTopicPath(topic)
      } catch {
        case _: NodeExistsException => throw new TopicAlreadyMarkedForDeletionException(
          "topic %s is already marked for deletion".format(topic))
        case e: Throwable => throw new AdminOperationException(e.getMessage)
       }
    } else {
      throw new UnknownTopicOrPartitionException(s"Topic `$topic` to delete does not exist")
    }
  }

  /**
  * Add partitions to existing topic with optional replica assignment. If no replica assignment is
  * specified (replicaAssignment is None), then we create a replica assignment keeping topic placement
  * constraint in mind (if a topic constraint is present for the topic).
  *
  * @param topic Topic for adding partitions to
  * @param existingAssignment A map from partition id to its assignment
  * @param allBrokers All brokers in the cluster
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignment Manual replica assignment, or none
  * @param validateOnly If true, validate the parameters without actually adding the partitions
  * @param topicPlacement Topic placement constraint for the topic.
  * @param clusterLink Optional cluster link topic state
  * @return the updated replica assignment
  */
  def addPartitions(topic: String,
                    existingAssignment: Map[Int, ReplicaAssignment],
                    allBrokers: Seq[BrokerMetadata],
                    numPartitions: Int = 1,
                    replicaAssignment: Option[Map[Int, ReplicaAssignment]] = None,
                    validateOnly: Boolean = false,
                    topicPlacement: Option[TopicPlacement] = None,
                    clusterLink: Option[ClusterLinkTopicState] = None): Map[Int, ReplicaAssignment] = {
    val existingAssignmentPartition0 = existingAssignment.getOrElse(0,
      throw new AdminOperationException(
        s"Unexpected existing replica assignment for topic '$topic', partition id 0 is missing. " +
          s"Assignment: $existingAssignment")).replicas

    val partitionsToAdd = numPartitions - existingAssignment.size
    if (partitionsToAdd <= 0)
      throw new InvalidPartitionsException(
        s"The number of partitions for a topic can only be increased. " +
          s"Topic $topic currently has ${existingAssignment.size} partitions, " +
          s"$numPartitions would not be an increase.")

    replicaAssignment.foreach { proposedReplicaAssignment =>
      validateReplicaAssignment(proposedReplicaAssignment, existingAssignmentPartition0.size,
        allBrokers.map(_.id).toSet)
    }

    val proposedAssignmentForNewPartitions = replicaAssignment.getOrElse {
      val startIndex = math.max(0, allBrokers.indexWhere(_.id >= existingAssignmentPartition0.head))

      Observer.getReplicaAssignment(allBrokers, topicPlacement, partitionsToAdd,
        existingAssignmentPartition0.size, startIndex, existingAssignment.size)
    }

    val proposedAssignment = existingAssignment ++ proposedAssignmentForNewPartitions
    if (!validateOnly) {
      info(s"Creating $partitionsToAdd partitions for '$topic' with the following replica assignment: " +
        s"$proposedAssignmentForNewPartitions.")

      writeTopicPartitionAssignment(topic, proposedAssignment, isUpdate = true, clusterLink = clusterLink)
    }

    proposedAssignment
  }

  private def validateReplicaAssignment(replicaAssignment: Map[Int, ReplicaAssignment],
                                        expectedReplicationFactor: Int,
                                        availableBrokerIds: Set[Int]): Unit = {
    replicaAssignment.foreach { case (partitionId, assignment) =>
      if (assignment.replicas.isEmpty)
        throw new InvalidReplicaAssignmentException(
          s"Cannot have replication factor of 0 for partition id $partitionId.")
      if (assignment.replicas.size != assignment.replicas.toSet.size)
        throw new InvalidReplicaAssignmentException(
          s"Duplicate brokers not allowed in replica assignment: " +
            s"${assignment.replicas.mkString(", ")} for partition id $partitionId.")
      if (!assignment.replicas.toSet.subsetOf(availableBrokerIds))
        throw new BrokerNotAvailableException(
          s"Some brokers specified for partition id $partitionId are not available. " +
            s"Specified brokers: ${assignment.replicas.mkString(", ")}, " +
            s"available brokers: ${availableBrokerIds.mkString(", ")}.")
    }

    val badRepFactors = replicaAssignment.collect {
      case (partition, assignment) if assignment.replicas.size != expectedReplicationFactor =>
        partition -> assignment.replicas.size
    }
    if (badRepFactors.nonEmpty) {
      val sortedBadRepFactors = badRepFactors.toSeq.sortBy { case (partitionId, _) => partitionId }
      val partitions = sortedBadRepFactors.map { case (partitionId, _) => partitionId }
      val repFactors = sortedBadRepFactors.map { case (_, rf) => rf }
      throw new InvalidReplicaAssignmentException(s"Inconsistent replication factor between partitions, " +
        s"partition 0 has $expectedReplicationFactor while partitions [${partitions.mkString(", ")}] have " +
        s"replication factors [${repFactors.mkString(", ")}], respectively.")
    }
  }

  def parseBroker(broker: String): Option[Int] = {
    broker match {
      case ConfigEntityName.Default => None
      case _ =>
        try Some(broker.toInt)
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"Error parsing broker $broker. The broker's Entity Name must be a single integer value")
        }
    }
  }

  /**
   * Change the configs for a given entityType and entityName
   * @param entityType
   * @param entityName
   * @param configs
   */
  def changeConfigs(entityType: String, entityName: String, configs: Properties): Unit = {

    entityType match {
      case ConfigType.Topic => changeTopicConfig(entityName, configs)
      case ConfigType.Client => changeClientIdConfig(entityName, configs)
      case ConfigType.User => changeUserOrUserClientIdConfig(entityName, configs)
      case ConfigType.Broker => changeBrokerConfig(parseBroker(entityName), configs)
      case ConfigType.ClusterLink =>
        throw new IllegalArgumentException("Cluster link configs can be altered only using Admin API")
      case _ => throw new IllegalArgumentException(s"$entityType is not a known entityType. Should be one of ${ConfigType.Topic}, ${ConfigType.Client}, ${ConfigType.User}, ${ConfigType.Broker}, or ${ConfigType.ClusterLink}.")
    }
  }

  /**
   * Update the config for a client and create a change notification so the change will propagate to other brokers.
   * If clientId is <default>, default clientId config is updated. ClientId configs are used only if <user, clientId>
   * and <user> configs are not specified.
   *
   * @param sanitizedClientId: The sanitized clientId for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeClientIdConfig(sanitizedClientId: String, configs: Properties): Unit = {
    DynamicConfig.Client.validate(configs)
    changeEntityConfig(ConfigType.Client, sanitizedClientId, configs)
  }

  /**
   * Update the config for a <user> or <user, clientId> and create a change notification so the change will propagate to other brokers.
   * User and/or clientId components of the path may be <default>, indicating that the configuration is the default
   * value to be applied if a more specific override is not configured.
   *
   * @param sanitizedEntityName: <sanitizedUserPrincipal> or <sanitizedUserPrincipal>/clients/<clientId>
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeUserOrUserClientIdConfig(sanitizedEntityName: String, configs: Properties): Unit = {
    if (sanitizedEntityName == ConfigEntityName.Default || sanitizedEntityName.contains("/clients"))
      DynamicConfig.Client.validate(configs)
    else
      DynamicConfig.User.validate(configs)
    changeEntityConfig(ConfigType.User, sanitizedEntityName, configs)
  }

  /**
   * validates the topic configs
   * @param topic
   * @param configs
   */
  def validateTopicConfig(topic: String, configs: Properties): Unit = {
    Topic.validate(topic)
    if (!zkClient.topicExists(topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))
    // remove the topic overrides
    LogConfig.validate(configs)
  }

  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   *
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
   def changeTopicConfig(topic: String, configs: Properties): Unit = {
    validateTopicConfig(topic, configs)
    changeEntityConfig(ConfigType.Topic, topic, configs)
  }

  /**
    * Override the broker config on some set of brokers. These overrides will be persisted between sessions, and will
    * override any defaults entered in the broker's config files
    *
    * @param brokers: The list of brokers to apply config changes to
    * @param configs: The config to change, as properties
    */
  def changeBrokerConfig(brokers: Seq[Int], configs: Properties): Unit = {
    validateBrokerConfig(configs)
    brokers.foreach {
      broker => changeEntityConfig(ConfigType.Broker, broker.toString, configs)
    }
  }

  /**
    * Override a broker override or broker default config. These overrides will be persisted between sessions, and will
    * override any defaults entered in the broker's config files
    *
    * @param broker: The broker to apply config changes to or None to update dynamic default configs
    * @param configs: The config to change, as properties
    */
  def changeBrokerConfig(broker: Option[Int], configs: Properties): Unit = {
    validateBrokerConfig(configs)
    changeEntityConfig(ConfigType.Broker, broker.map(_.toString).getOrElse(ConfigEntityName.Default), configs)
  }

  /**
    * Validate dynamic broker configs. Since broker configs may contain custom configs, the validation
    * only verifies that the provided config does not contain any static configs.
    * @param configs configs to validate
    */
  def validateBrokerConfig(configs: Properties): Unit = {
    DynamicConfig.Broker.validate(configs)
  }

  /**
    * Update the config for an existing cluster link and create a change notification so the change will propagate to
    * other brokers.
    *
    * @param linkId the link ID for cluster link whose config is being changed
    * @param persistentProps the configs to change
    */
  def changeClusterLinkConfig(linkId: UUID, persistentProps: Properties): Unit = {
    ensureClusterLinkExists(linkId)
    changeEntityConfig(ConfigType.ClusterLink, linkId.toString, persistentProps)
  }

  /**
    * Returns persistent properties of cluster link.
    * @param linkId the link ID for cluster link whose config is returned
    */
  def fetchClusterLinkConfig(linkId: UUID): Properties = {
    fetchEntityConfig(ConfigType.ClusterLink, linkId.toString)
  }

  /**
    * Verifies that cluster link with the specified ID exists.
    *
    * @param linkId the link ID for cluster link
    */
  def ensureClusterLinkExists(linkId: UUID): Unit = {
    if (!zkClient.clusterLinkExists(linkId))
      throw new ClusterLinkNotFoundException(s"Cluster link with ID '$linkId' does not exist.")
  }

  private def changeEntityConfig(rootEntityType: String, fullSanitizedEntityName: String, configs: Properties): Unit = {
    val sanitizedEntityPath = rootEntityType + '/' + fullSanitizedEntityName
    zkClient.setOrCreateEntityConfigs(rootEntityType, fullSanitizedEntityName, configs)

    // create the change notification
    zkClient.createConfigChangeNotification(sanitizedEntityPath)
  }

  /**
   * Read the entity (topic, broker, client, user or <user, client>) config (if any) from zk
   * sanitizedEntityName is <topic>, <broker>, <client-id>, <user>, <user>/clients/<client-id>, or <cluster-link>.
   * @param rootEntityType
   * @param sanitizedEntityName
   * @return
   */
  def fetchEntityConfig(rootEntityType: String, sanitizedEntityName: String): Properties = {
    zkClient.getEntityConfigs(rootEntityType, sanitizedEntityName)
  }

  /**
   * Gets all topic configs
   * @return
   */
  def getAllTopicConfigs(): Map[String, Properties] =
    zkClient.getAllTopicsInCluster().map(topic => (topic, fetchEntityConfig(ConfigType.Topic, topic))).toMap

  /**
   * Gets all the entity configs for a given entityType
   * @param entityType
   * @return
   */
  def fetchAllEntityConfigs(entityType: String): Map[String, Properties] =
    zkClient.getAllEntitiesWithConfig(entityType).map(entity => (entity, fetchEntityConfig(entityType, entity))).toMap

  /**
   * Gets all the entity configs for a given childEntityType
   * @param rootEntityType
   * @param childEntityType
   * @return
   */
  def fetchAllChildEntityConfigs(rootEntityType: String, childEntityType: String): Map[String, Properties] = {
    def entityPaths(rootPath: Option[String]): Seq[String] = {
      val root = rootPath match {
        case Some(path) => rootEntityType + '/' + path
        case None => rootEntityType
      }
      val entityNames = zkClient.getAllEntitiesWithConfig(root)
      rootPath match {
        case Some(path) => entityNames.map(entityName => path + '/' + entityName)
        case None => entityNames
      }
    }
    entityPaths(None)
      .flatMap(entity => entityPaths(Some(entity + '/' + childEntityType)))
      .map(entityPath => (entityPath, fetchEntityConfig(rootEntityType, entityPath))).toMap
  }

  /**
   * Gets the partitions for the given topics
   * @param topics the topics whose partitions we wish to get.
   * @return the partition count for each topic from the given topics.
   */
  def numPartitions(topics: Set[String]): Map[String, Int] = {
    zkClient.getPartitionsForTopics(topics).map { case (topic, partitions) =>
      (topic, partitions.size)
    }
  }

  /**
    * Creates a new cluster link.
    *
    * @param clusterLinkData the cluster link's data
    * @param persistentConfigs the initial configuration properties
    */
  def createClusterLink(clusterLinkData: ClusterLinkData, persistentConfigs: Properties): Unit = {
    val linkId = clusterLinkData.linkId
    if (zkClient.clusterLinkExists(linkId))
      throw new ClusterLinkExistsException(s"Cluster link with ID '$linkId' already exists.")

    // Write the cluster link's config before creating the cluster link object. For correctness reasons,
    // it must be done in this order, because the metadata object is the source of truth for the link's
    // existence. If, upon broker recovery, a cluster link config exists without a corresponding metadata
    // object, then it is known that the cluster link was either deleted or never fully created.
    zkClient.setOrCreateEntityConfigs(ConfigType.ClusterLink, linkId.toString, persistentConfigs)
    zkClient.createClusterLink(clusterLinkData)
    zkClient.createConfigChangeNotification(ConfigType.ClusterLink + '/' + linkId)
  }

  /**
    * Retrieves the requested cluster link's data.
    *
    * @param linkId the link ID to retrieve
    * @return the corresponding cluster link data, or none if it doesn't exist
    */
  def getClusterLink(linkId: UUID): Option[ClusterLinkData] =
    zkClient.getClusterLinks(Set(linkId)).get(linkId)

  /**
   * Gets cluster link data for a set of link IDs.
   *
   * @param linkIds set of cluster link IDs
   * @return a map of link IDs with data
   */
  def getClusterLinks(linkIds: Set[UUID]): Map[UUID, ClusterLinkData] =
    zkClient.getClusterLinks(linkIds)

  /**
    * Retrieves all of the cluster links, returning their data.
    *
    * @return the cluster links' data
    */
  def getAllClusterLinks(): Seq[ClusterLinkData] =
    zkClient.getClusterLinks(zkClient.getChildren(ClusterLinksZNode.path).map(UUID.fromString).toSet).values.toSeq

  /**
    * Sets the cluster link to the provided data.
    *
    * @param clusterLinkData the cluster link's data
    */
  def setClusterLink(clusterLinkData: ClusterLinkData): Unit = {
    val linkId = clusterLinkData.linkId
    ensureClusterLinkExists(linkId)
    zkClient.setClusterLink(clusterLinkData)
    zkClient.createConfigChangeNotification(ConfigType.ClusterLink + '/' + linkId)
  }

  /**
    * Deletes a cluster link by removing its metadata.
    *
    * @param linkId the ID of the cluster link to delete
    */
  def deleteClusterLink(linkId: UUID): Unit = {
    ensureClusterLinkExists(linkId)

    // Since the cluster link metadata object is the source of truth for the cluster link, delete
    // it first before the config to ensure correctness.
    zkClient.deleteClusterLink(linkId)
    zkClient.deleteEntityConfig(ConfigType.ClusterLink, linkId.toString)
    zkClient.createConfigChangeNotification(ConfigType.ClusterLink + '/' + linkId)
  }

}
