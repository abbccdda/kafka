/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.ConfigException
import org.junit.Assert._
import org.junit.Test

class ClusterLinkConfigTest {

  @Test
  def testTenantPrefix(): Unit = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    props.put(ClusterLinkConfig.TenantPrefixProp, "prefix")
    assertThrows(classOf[ConfigException], () => ClusterLinkConfig.validate(props))
    assertThrows(classOf[ConfigException], () => new ClusterLinkConfig(props))

    props.put(ClusterLinkConfig.TenantPrefixProp, "")
    assertThrows(classOf[ConfigException], () => ClusterLinkConfig.validate(props))
    assertThrows(classOf[ConfigException], () => new ClusterLinkConfig(props))

    props.remove(ClusterLinkConfig.TenantPrefixProp)
    ClusterLinkConfig.validate(props)
    val config = new ClusterLinkConfig(props)
    assertNull(config.getString(ClusterLinkConfig.TenantPrefixProp))
  }
}