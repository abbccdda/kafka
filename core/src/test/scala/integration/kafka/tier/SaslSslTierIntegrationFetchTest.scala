/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier

import java.io.File

import kafka.api.{KafkaSasl, SaslSetup}
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{After, Before}

class SaslSslTierIntegrationFetchTest extends TierIntegrationFetchTest with SaslSetup {
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")

  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  override protected val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }
}
