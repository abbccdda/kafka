/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.server.ConfigType
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.common.{KafkaFuture, MetricName}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metrics.stats.{Rate, CumulativeSum}
import org.apache.kafka.common.metrics.{Metrics, Sensor}

import scala.jdk.CollectionConverters._
import scala.collection.Map
import scala.collection.mutable

/**
  * Task that periodically fetches remote topics' configurations and synchronizes any necessary
  * changes to the local topics' configurations.
  */
class ClusterLinkSyncTopicsConfigs(clientManager: ClusterLinkClientManager, syncIntervalMs: Int,
                                   metrics: Metrics,
                                   metricsTags: java.util.Map[String, String])
    extends ClusterLinkScheduler.PeriodicTask(clientManager.scheduler, name = "SyncTopicsConfigs", syncIntervalMs) {

  // Maps a topic to the remote topic's configuration, otherwise none if unknown.
  private val configs = mutable.Map[String, Option[Config]]()
  private var topicConfigUpdateSensor: Sensor = _

  override def startup(): Unit = {
    topicConfigUpdateSensor = metrics.sensor("topic-config-update-sensor")
    val topicConfigUpdateTotal = new MetricName("topic-config-update-total",
      "cluster-link-metrics", "Total number of topic config updates.",
      metricsTags)
    val topicConfigUpdateRate = new MetricName("topic-config-update-rate",
      "cluster-link-metrics", "Rate of topic config updates.", metricsTags)
    topicConfigUpdateSensor.add(topicConfigUpdateTotal, new CumulativeSum)
    topicConfigUpdateSensor.add(topicConfigUpdateRate, new Rate)
    super.startup()
  }

  override def shutdown(): Unit = {
    metrics.removeSensor("topic-config-update-sensor")
    super.shutdown()
  }

  override protected def run(): Boolean = {
    // Get the cluster link's topics and resolve any differences.
    val newTopics = clientManager.getTopics
    val oldTopics = configs.keySet
    oldTopics.diff(newTopics).foreach(configs.remove)
    newTopics.diff(oldTopics).foreach(topic => configs.put(topic, None))

    val resources = configs.keys.map(name => new ConfigResource(ConfigResource.Type.TOPIC, name)).toSet
    if (resources.nonEmpty) {
      val describeConfigsResult = clientManager.getAdmin.describeConfigs(resources.asJava)
      scheduleWhenComplete(describeConfigsResult.all, () => handleTopicConfigs(describeConfigsResult.values.asScala.toMap))
      false
    } else {
      true
    }
  }

  private def handleTopicConfigs(result: Map[ConfigResource, KafkaFuture[Config]]): Boolean = {
    result.foreach { case (resource, future) =>
      val topic = resource.name
      configs.get(topic).foreach { oldConfig =>
        try {
          val curConfig = future.get
          if (oldConfig.forall(_ != curConfig)) {
            debug(s"Detected new remote configuration for mirror topic '$topic' on cluster link '${clientManager.linkData.linkName}'")

            // Determine if the local configuration has changed, and if so, update it.
            val curProps = clientManager.adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
            val newProps = ClusterLinkUtils.updateMirrorProps(topic, curProps, curConfig)
            if (newProps != curProps) {
              debug(s"Updating local configuration for mirror topic '$topic' on cluster link '${clientManager.linkData.linkName}'")
              clientManager.adminZkClient.changeTopicConfig(topic, newProps)
            }

            configs.put(topic, Some(curConfig))
            topicConfigUpdateSensor.record()
          }
        } catch {
          case e: Throwable =>
            debug(s"Error encountered while processing remote configuration for mirror topic '$topic' " +
              s"on cluster link ${clientManager.linkData.linkName}'", e)
        }
      }
    }

    true
  }
}
