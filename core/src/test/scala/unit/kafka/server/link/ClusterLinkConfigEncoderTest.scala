/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.Properties

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.provider.ConfigProvider
import org.apache.kafka.common.config.{AbstractConfig, ConfigData, SslConfigs}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class ClusterLinkConfigEncoderTest {

  @Test
  def testEncodingWithSensitiveConfigs(): Unit = {
    val props = new Properties
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore.jks")
    props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "keystore-secret")
    props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "key-secret")
    val sensitiveConfigs = Set(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslConfigs.SSL_KEY_PASSWORD_CONFIG)

    verifyEncodeDecode(props, sensitiveConfigs, props)
  }

  @Test
  def testEncodingNoSensitiveConfigs(): Unit = {
    val props = new Properties
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")

    verifyEncodeDecode(props, Set.empty, props)
  }

  @Test
  def testExternalizedConfigs(): Unit = {
    val props = new Properties
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    props.setProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "mockvault")
    props.setProperty(s"${AbstractConfig.CONFIG_PROVIDERS_CONFIG}.mockvault.class", classOf[MockVault].getName)
    props.setProperty(s"${AbstractConfig.CONFIG_PROVIDERS_CONFIG}.mockvault.param.url", MockVault.Url)
    props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, s"$${mockvault:/clusterlink/test:${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}}")
    props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, s"$${mockvault:/clusterlink/test:${SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG}}")
    props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, s"$${mockvault:/clusterlink/test:${SslConfigs.SSL_KEY_PASSWORD_CONFIG}}")
    val sensitiveConfigs = Set(
      AbstractConfig.CONFIG_PROVIDERS_CONFIG,
      s"${AbstractConfig.CONFIG_PROVIDERS_CONFIG}.mockvault.class",
      s"${AbstractConfig.CONFIG_PROVIDERS_CONFIG}.mockvault.param.url",
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_KEY_PASSWORD_CONFIG
    )
    val externalizedConfigs = Set(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_KEY_PASSWORD_CONFIG)

    val resolvedProps = new Properties
    props.asScala.foreach { case (k, v) =>
      resolvedProps.setProperty(k,
        if (externalizedConfigs.contains(k)) MockVault.Configs(k) else props.getProperty(k))
    }

    verifyEncodeDecode(props, sensitiveConfigs, resolvedProps)
  }

  def verifyEncodeDecode(props: Properties, sensitiveConfigs: Set[String], resolvedProps: Properties): Unit = {
    val linkConfig = new ClusterLinkConfig(props)

    val brokerConfig1 = brokerConfig(secret = None)
    val encoder1 = new ClusterLinkConfigEncoder(brokerConfig1)
    if (sensitiveConfigs.isEmpty) {
      verifyEncoding(brokerConfig1, linkConfig, sensitiveConfigs, resolvedProps)
    } else {
      assertThrows(classOf[InvalidConfigurationException], () => encoder1.encode(props))
    }

    val brokerConfig2 = brokerConfig(secret = Some("secret-1234"))
    verifyEncoding(brokerConfig2, linkConfig, sensitiveConfigs, resolvedProps)

    val brokerConfig3 = brokerConfig(secret = Some("secret-new"), oldSecret = Some("secret-old"))
    val encoded = verifyEncoding(brokerConfig3, linkConfig, sensitiveConfigs, resolvedProps)
    verifyDecoding(brokerConfig(secret = Some("secret-new")), encoded, resolvedProps)
    verifyDecoding(brokerConfig(secret = Some("secret-old")), encoded, resolvedProps)
  }

  private def brokerConfig(secret: Option[String], oldSecret: Option[String] = None): KafkaConfig = {
    val brokerProps = TestUtils.createBrokerConfig(1, "localhost:2181")
    secret.foreach(brokerProps.setProperty(KafkaConfig.PasswordEncoderSecretProp, _))
    oldSecret.foreach(brokerProps.setProperty(KafkaConfig.PasswordEncoderOldSecretProp, _))
    new KafkaConfig(brokerProps)
  }

  private def verifyEncoding(kafkaConfig: KafkaConfig,
                             linkConfig: ClusterLinkConfig,
                             sensitiveConfigs: Set[String],
                             resolvedProps: Properties): Properties = {
    val encoder = new ClusterLinkConfigEncoder(kafkaConfig)
    val props = new Properties
    linkConfig.originalsStrings.forEach((k, v) => props.setProperty(k, v))
    val encodedProps = encoder.encode(props)
    sensitiveConfigs.foreach { name =>
      assertNotEquals(linkConfig.originals.get(name), encodedProps.getProperty(name))
    }
    linkConfig.originals.asScala.keySet.diff(sensitiveConfigs).foreach { name =>
      assertEquals(linkConfig.originals.get(name), encodedProps.getProperty(name))
    }
    verifyDecoding(kafkaConfig, encodedProps, resolvedProps)
    encodedProps
  }

  private def verifyDecoding(kafkaConfig: KafkaConfig,
                             encodedProps: Properties,
                             resolvedProps: Properties): Unit = {
    val encoder = new ClusterLinkConfigEncoder(kafkaConfig)
    val linkConfig = encoder.clusterLinkConfig(encodedProps)
    assertEquals(resolvedProps, linkConfig.originals)
  }
}

object MockVault {
  val Url = "https://mockvault"
  val Configs = Map(
    SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> "/path/to/keystore.jks",
    SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> "keystore-password",
    SslConfigs.SSL_KEY_PASSWORD_CONFIG -> "key-password"
  )
}

class MockVault extends ConfigProvider {
  override def configure(configs: util.Map[String, _]): Unit = {
    assertEquals(MockVault.Url, configs.get("url"))
  }

  override def get(path: String): ConfigData = {
    new ConfigData(MockVault.Configs.asJava)
  }

  override def get(path: String, keys: util.Set[String]): ConfigData = {
    get(path)
  }

  override def close(): Unit = {}
}
