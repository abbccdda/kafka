/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaYammerMetrics
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.utils.Exit
import org.apache.zookeeper.client.ZKClientConfig
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._

class KafkaServerTest extends ZooKeeperTestHarness {

  val exited = new AtomicBoolean(false)

  @Before
  override def setUp(): Unit = {
    Exit.setExitProcedure { (_, _) =>
      exited.set(true)
      throw new Exception()
    }
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    Exit.resetExitProcedure()
    KafkaServer.externalShutdownInitiations.set(0L)
  }

  @Test
  def testShutdown(): Unit = {
    new KafkaServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, zkConnect)))

    assertFalse("Exit produce should not have been executed yet", exited.get())
    val th = KafkaServer.initiateShutdown()
    th.join()

    assertTrue("Exit produce should have been executed", exited.get())
    assertEquals(1, KafkaServer.externalShutdownInitiations.get())
    assertEquals(1, metricValue("ExternalShutdownInitiations"))
  }

  @Test
  def testTwoConsecutiveShutdownCallExitTwice(): Unit = {
    new KafkaServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, zkConnect)))

    assertFalse("Exit produce should not have been executed yet", exited.get())
    val th1 = KafkaServer.initiateShutdown()
    val th2 = KafkaServer.initiateShutdown()
    th1.join()
    th2.join()

    assertTrue("Exit produce should have been executed", exited.get())
    assertEquals(2, KafkaServer.externalShutdownInitiations.get())
    assertEquals(2, metricValue("ExternalShutdownInitiations"))
  }

  private def metricValue(name: String): Long = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter(_._1.getName == name)
      .values.headOption.getOrElse(throw new Exception(s"Could not find metric $name"))
      .asInstanceOf[Gauge[Long]].value()
  }

  @Test
  def testAlreadyRegisteredAdvertisedListeners(): Unit = {
    //start a server with a advertised listener
    val server1 = createServer(1, "myhost", TestUtils.RandomPort)

    //start a server with same advertised listener
    intercept[IllegalArgumentException] {
      createServer(2, "myhost", TestUtils.boundPort(server1))
    }

    //start a server with same host but with different port
    val server2 = createServer(2, "myhost", TestUtils.RandomPort)

    TestUtils.shutdownServers(Seq(server1, server2))
  }

  @Test
  def testCreatesProperZkTlsConfigWhenDisabled(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    props.put(KafkaConfig.ZkSslClientEnableProp, "false")
    assertEquals(None, KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props)))
  }

  @Test
  def testCreatesProperZkTlsConfigWithTrueValues(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "true"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "HTTPS"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig: Option[ZKClientConfig] = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "true"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "true"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.get.getProperty(KafkaConfig.ZkSslConfigToSystemPropertyMap(kafkaProp))))
  }

  @Test
  def testCreatesProperZkTlsConfigWithFalseAndListValues(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp => "true"
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "false"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => ""
      case KafkaConfig.ZkSslEnabledProtocolsProp | KafkaConfig.ZkSslCipherSuitesProp => "A,B"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig: Option[ZKClientConfig] = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp => "true"
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "false"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "false"
      case KafkaConfig.ZkSslEnabledProtocolsProp | KafkaConfig.ZkSslCipherSuitesProp => "A,B"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.get.getProperty(KafkaConfig.ZkSslConfigToSystemPropertyMap(kafkaProp))))
  }

  def createServer(nodeId: Int, hostName: String, port: Int): KafkaServer = {
    val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
    props.put(KafkaConfig.AdvertisedListenersProp, s"PLAINTEXT://$hostName:$port")
    val kafkaConfig = KafkaConfig.fromProps(props)
    TestUtils.createServer(kafkaConfig)
  }

}
