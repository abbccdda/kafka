/*
 Copyright 2020 Confluent Inc.
 */
package unit.kafka.server

import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.{BaseRequestTest, KafkaServer}
import kafka.utils.TestUtils
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{InitiateShutdownRequest, InitiateShutdownResponse}
import org.apache.kafka.common.utils.Exit
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertFalse}

class InitiateShutdownRequestIntegrationTest extends BaseRequestTest {
  override val brokerCount: Int = 1
  var server: KafkaServer = _
  var brokerEpoch: Long = _
  val exited = new AtomicBoolean(false)

  @Before
  override def setUp(): Unit = {
    Exit.setExitProcedure((_, _) => {
      exited.set(true)
      throw new Exception()
    })
    super.setUp()

    server = servers.head
    brokerEpoch = server.kafkaController.brokerEpoch
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    Exit.resetExitProcedure()
    KafkaServer.externalShutdownInitiations.set(0L)
  }

  @Test
  def testInitiateShutdownRequest(): Unit = {
    val shutdownReq = new InitiateShutdownRequest.Builder(brokerEpoch).build()
    val shutdownResponse = connectAndReceive[InitiateShutdownResponse](shutdownReq, destination = controllerSocketServer)

    assertEquals(Errors.NONE.code(), shutdownResponse.data().errorCode())
    TestUtils.waitUntilTrue(() => exited.get(), "Shutdown should have been initiated")
  }

  @Test
  def testInitiateShutdownRequestReturnsStaleBrokerEpochExceptionIfBrokerEpochIsStale(): Unit = {
    val shutdownReq = new InitiateShutdownRequest.Builder(brokerEpoch - 1).build()
    val shutdownResponse = connectAndReceive[InitiateShutdownResponse](shutdownReq, destination = controllerSocketServer)

    assertEquals(Errors.STALE_BROKER_EPOCH.code(), shutdownResponse.data().errorCode())
    assertFalse("Shutdown should not have been initiated", exited.get())
  }
}
