/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import org.apache.kafka.clients.admin.AdminClientConfig
import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class ClusterLinkConfigTest {

  @Test
  def testConfigCategories(): Unit = {
    val allProps = ClusterLinkConfig.configNames.toSet
    val replicationProps = ClusterLinkConfig.ReplicationProps
    val migrationProps = ClusterLinkConfig.PeriodicMigrationProps
    val notCategorizedProps = allProps -- replicationProps -- migrationProps -
      ClusterLinkConfig.ClusterLinkPausedProp -- AdminClientConfig.configNames.asScala
    assertEquals(Set.empty, notCategorizedProps)
  }
}
