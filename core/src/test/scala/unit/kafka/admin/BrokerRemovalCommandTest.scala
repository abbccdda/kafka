/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.admin

import java.util

import joptsimple.OptionException
import kafka.common.AdminCommandFailedException
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.BrokerRemovalDescription.{BrokerShutdownStatus, PartitionReassignmentsStatus}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.errors.BrokerRemovalInProgressException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers.anyList
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.Assertions.intercept


final class BrokerRemovalCommandTest {
  val brokerId = 0
  val bootstrapServer = "localhost:9092"
  val brokerIds: util.ArrayList[Integer] = new util.ArrayList[Integer]() {
    {
      add(brokerId)
    }
  }

  private def runCommand(args: Array[String], mockAdminClient: MockAdminClient): String = {
    TestUtils.grabConsoleOutput(BrokerRemovalCommand.run(
      Array("--bootstrap-server", bootstrapServer, "--broker-id", brokerId.toString) ++ args, Some(mockAdminClient)))
  }

  @Test
  def testDeleteBroker_successful(): Unit = {
    val admin = mock(classOf[MockAdminClient])
    val removeBrokersResult = mock(classOf[RemoveBrokersResult])
    val deleteBrokerResult = mock(classOf[KafkaFutureImpl[util.List[Integer]]])
    when(deleteBrokerResult.get()).thenReturn(brokerIds)
    when(removeBrokersResult.all()).thenReturn(deleteBrokerResult)
    when(admin.removeBrokers(anyList())).thenReturn(removeBrokersResult)

    val output = runCommand(Array("--delete"), admin)

    verify(admin).removeBrokers(brokerIds)
    assertTrue(output.contains(brokerId.toString))
  }

  @Test
  def testDeleteBroker_throwsException(): Unit = {
    val admin = mock(classOf[MockAdminClient])
    val removeBrokersResult = mock(classOf[RemoveBrokersResult])
    val deleteBrokerResult = mock(classOf[KafkaFutureImpl[util.List[Integer]]])
    when(deleteBrokerResult.get()).thenThrow(classOf[BrokerRemovalInProgressException])
    when(removeBrokersResult.all()).thenReturn(deleteBrokerResult)
    when(admin.removeBrokers(anyList())).thenReturn(removeBrokersResult)

    assertThrows(
      s"Broker $brokerId is already being removed",
      classOf[AdminCommandFailedException], () => runCommand(Array("--delete"), admin))
  }

  @Test
  def testDescribeBrokerRemoval(): Unit = {
    val admin = mock(classOf[MockAdminClient])
    val describeBrokerRemovalsResult = mock(classOf[DescribeBrokerRemovalsResult])
    val brokerRemovalResults = mock(classOf[KafkaFutureImpl[util.Map[Integer, BrokerRemovalDescription]]])
    when(describeBrokerRemovalsResult.descriptions()).thenReturn(brokerRemovalResults)
    val removalResult = new util.HashMap[Integer, BrokerRemovalDescription]()
    removalResult.put(brokerId, new BrokerRemovalDescription(
      brokerId, BrokerShutdownStatus.COMPLETE, PartitionReassignmentsStatus.COMPLETE, null))
    when(brokerRemovalResults.get()).thenReturn(removalResult)
    when(admin.describeBrokerRemovals()).thenReturn(describeBrokerRemovalsResult)

    val output = runCommand(Array("--describe"), admin)

    verify(admin).describeBrokerRemovals()
    assertTrue(output.contains(brokerId.toString))
    assertTrue(output.contains(BrokerShutdownStatus.COMPLETE.toString))
  }

  @Test
  def testBadCommands(): Unit = {
    intercept[IllegalArgumentException] {
      new BrokerRemovalCommandOptions(Array("--bootstrap-server", "localhost:9092")).verifyArgs()
    }
    intercept[OptionException] {
      new BrokerRemovalCommandOptions(Array("--bootstrap-server", "localhost:9092", "--not-a-command")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new BrokerRemovalCommandOptions(Array("--bootstrap-server", "localhost:9092", "--delete", "--describe")).verifyArgs()
    }
  }

  @Test
  def testMissingRequiredArgs(): Unit = {
    intercept[IllegalArgumentException] {
      new BrokerRemovalCommandOptions(Array("--describe")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new BrokerRemovalCommandOptions(Array("--bootstrap-server", "localhost:9092", "--describe")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new BrokerRemovalCommandOptions(Array("--bootstrap-server", "localhost:9092", "--delete")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new BrokerRemovalCommandOptions(Array("--broker-id", brokerId.toString, "--delete")).verifyArgs()
    }
  }
}
