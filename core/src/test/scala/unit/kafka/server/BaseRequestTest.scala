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

package kafka.server

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.Properties

import kafka.api.IntegrationTestHarness
import kafka.log.AbstractLog
import kafka.network.SocketServer
import kafka.utils.NotNothing
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ListOffsetRequest, ListOffsetResponse, RequestHeader, ResponseHeader}
import org.junit.Assert.{assertFalse, assertTrue}

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.reflect.ClassTag

abstract class BaseRequestTest extends IntegrationTestHarness {
  private var correlationId = 0

  // If required, set number of brokers
  override def brokerCount: Int = 3

  // If required, override properties by mutating the passed Properties object
  protected def brokerPropertyOverrides(properties: Properties): Unit = {}

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach { p =>
      p.put(KafkaConfig.ControlledShutdownEnableProp, "false")
      brokerPropertyOverrides(p)
    }
  }

  def anySocketServer: SocketServer = {
    servers.find { server =>
      val state = server.brokerState.currentState
      state != NotRunning.state && state != BrokerShuttingDown.state
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No live broker is available"))
  }

  def controllerSocketServer: SocketServer = {
    servers.find { server =>
      server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No controller broker is available"))
  }

  def notControllerSocketServer: SocketServer = {
    servers.find { server =>
      !server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No non-controller broker is available"))
  }

  def brokerSocketServer(brokerId: Int): SocketServer = {
    servers.find { server =>
      server.config.brokerId == brokerId
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId"))
  }

  def connect(socketServer: SocketServer = anySocketServer,
              listenerName: ListenerName = listenerName): Socket = {
    new Socket("localhost", socketServer.boundPort(listenerName))
  }

  private def sendRequest(socket: Socket, request: Array[Byte]): Unit = {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  def receive[T <: AbstractResponse](socket: Socket, apiKey: ApiKeys, version: Short)
                                    (implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()

    val responseBytes = new Array[Byte](len)
    incoming.readFully(responseBytes)

    val responseBuffer = ByteBuffer.wrap(responseBytes)
    ResponseHeader.parse(responseBuffer, apiKey.responseHeaderVersion(version))

    val responseStruct = apiKey.parseResponse(version, responseBuffer)
    AbstractResponse.parseResponse(apiKey, responseStruct, version) match {
      case response: T => response
      case response =>
        throw new ClassCastException(s"Expected response with type ${classTag.runtimeClass}, but found ${response.getClass}")
    }
  }

  def sendAndReceive[T <: AbstractResponse](request: AbstractRequest,
                                            socket: Socket,
                                            clientId: String = "client-id",
                                            correlationId: Option[Int] = None)
                                           (implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    send(request, socket, clientId, correlationId)
    receive[T](socket, request.api, request.version)
  }

  def connectAndReceive[T <: AbstractResponse](request: AbstractRequest,
                                               destination: SocketServer = anySocketServer,
                                               listenerName: ListenerName = listenerName)
                                              (implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    val socket = connect(destination, listenerName)
    try sendAndReceive[T](request, socket)
    finally socket.close()
  }

  /**
    * Serializes and sends the request to the given api.
    */
  def send(request: AbstractRequest,
           socket: Socket,
           clientId: String = "client-id",
           correlationId: Option[Int] = None): Unit = {
    val header = nextRequestHeader(request.api, request.version, clientId, correlationId)
    sendWithHeader(request, header, socket)
  }

  def sendWithHeader(request: AbstractRequest, header: RequestHeader, socket: Socket): Unit = {
    val serializedBytes = request.serialize(header).array
    sendRequest(socket, serializedBytes)
  }

  def nextRequestHeader[T <: AbstractResponse](apiKey: ApiKeys,
                                               apiVersion: Short,
                                               clientId: String = "client-id",
                                               correlationIdOpt: Option[Int] = None): RequestHeader = {
    val correlationId = correlationIdOpt.getOrElse {
      this.correlationId += 1
      this.correlationId
    }
    new RequestHeader(apiKey, apiVersion, clientId, correlationId)
  }

  protected def makeListOffsetsRequestAndValidateResponse(topicPartition: TopicPartition, timestamp: Long,
                                                          leaderId: Int, log: AbstractLog, version: Short): Unit = {
    val targetTimes = Map(topicPartition -> new ListOffsetRequest.PartitionData(timestamp, Int.MaxValue)).asJava
    val request = ListOffsetRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)
      .build(version)
    val response = connectAndReceive[ListOffsetResponse](request, brokerSocketServer(leaderId))
    assertTrue("Error returned by ListOffsetRequest", response.responseData().get(topicPartition).error == Errors.NONE)
    val offsets = asScalaBuffer(response.responseData().get(topicPartition).offsets)
    assertFalse("Offsets in ListOffsetResponse must fall within the range [logStartOffset, logEndOffset]",
      offsets.exists(_ < log.logStartOffset) || offsets.exists(_ > log.logEndOffset))
    assertTrue("Expected logStartOffset to be present at ListOffsetResponse", offsets.contains(log.logStartOffset))
    timestamp match {
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        assertTrue("Expected a single offset to be returned when querying for EARLIEST_TIMESTAMP", offsets.size == 1)
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        assertTrue("Expected log end offset to be returned when querying for LATEST_TIMESTAMP", offsets.head == log.logEndOffset)
    }
  }
}
