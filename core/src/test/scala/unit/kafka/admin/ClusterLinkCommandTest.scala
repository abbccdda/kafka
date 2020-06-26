/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.admin

import java.util.{Collection, Collections, Optional, UUID}

import joptsimple.OptionException
import kafka.server.link.ClusterLinkConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{CreateClusterLinksOptions, CreateClusterLinksResult, DeleteClusterLinksOptions, DeleteClusterLinksResult, ListClusterLinksOptions, ListClusterLinksResult, MockAdminClient}
import org.apache.kafka.common.{KafkaFuture, Node}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.requests.{ClusterLinkListing, NewClusterLink}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._

final class ClusterLinkCommandTest {
  val completed = "successfully completed"
  val validated = "successfully validated"

  val offsetJson: String =
    """{
      |"groupFilters": [{
      |     "name": "*",
      |     "patternType": "LITERAL",
      |     "filterType": "INCLUDE"
      |  },
      |  {
      |     "name": "excluded",
      |     "patternType": "PREFIXED",
      |     "filterType": "EXCLUDE"
      |  }]
      | }""".stripMargin

  val aclJson: String =
    """{
      | "aclFilters": [{
      |  "resourceFilter": {
      |      "resourceType": "any",
      |      "patternType": "any"
      |    },
      |  "accessFilter": {
      |     "operation": "any",
      |     "permissionType": "any"
      |    }
      |  }]
      | }""".stripMargin

  private def runCommand(args: Array[String], mockAdminClient: TestAdminClient): String = {
    TestUtils.grabConsoleOutput(ClusterLinkCommand.run(Array("--bootstrap-server", "localhost:9092") ++ args, Some(mockAdminClient)))
  }

  private def createClusterLinks(args: Array[String], expectValidateOnly: Boolean = false, expectValidateLink: Boolean = true,
                                 additionalConfigs: Map[String,String] = Map.empty, expectedConfigs: Map[String,String] = Map.empty): Unit = {
    val linkName = "test-link"
    val clusterId = "test-cluster-id"
    var issuedCommand = false

    val future = new KafkaFutureImpl[Void]
    future.complete(null)

    val result: CreateClusterLinksResult = EasyMock.createNiceMock(classOf[CreateClusterLinksResult])
    EasyMock.expect(result.all()).andReturn(future.asInstanceOf[KafkaFuture[Void]]).once()

    val configs = Map("bootstrap.servers" -> "10.20.30.40:9092", "request.timeout.ms" -> "100000") ++ additionalConfigs

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new TestAdminClient(node) {
      override def createClusterLinks(clusterLinks: Collection[NewClusterLink], options: CreateClusterLinksOptions): CreateClusterLinksResult = {
        issuedCommand = true
        assertEquals(1, clusterLinks.size)
        val newClusterLink = clusterLinks.iterator.next
        assertEquals(linkName, newClusterLink.linkName)
        assertEquals(clusterId, newClusterLink.clusterId)
        assertEquals((configs ++ expectedConfigs).asJava, newClusterLink.configs)
        assertEquals(expectValidateOnly, options.validateOnly)
        assertEquals(expectValidateLink, options.validateLink)
        result
      }
    }

    EasyMock.replay(result)
    val output = runCommand(Array("--create", "--link-name", linkName, "--cluster-id", clusterId,
      "--config", configs.map(kv => kv._1 + "=" + kv._2).mkString(","))
      ++ args, mockAdminClient)
    assertTrue(issuedCommand)
    EasyMock.reset(result)

    val expectedOutput = if (expectValidateOnly) validated else completed
    assertTrue(output.contains(expectedOutput))
  }

  @Test
  def testCreateClusterLinksWithOffsetSyncConfigs(): Unit = {
    createClusterLinks(args = Array("--consumer-group-filters-json",offsetJson),
      additionalConfigs = Map(ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetJson)
    )
    createClusterLinks(args = Array("--consumer-group-filters-json",offsetJson,"--validate-only"),
      additionalConfigs = Map(ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetJson),
      expectValidateOnly = true
    )
    createClusterLinks(args = Array("--consumer-group-filters-json",offsetJson,"--exclude-validate-link"),
      additionalConfigs = Map(ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetJson),
      expectValidateLink = false
    )
  }

  @Test
  def testCreateClusterLinksWithAclSyncConfigs(): Unit = {
    createClusterLinks(args = Array("--acl-filters-json", aclJson),
      additionalConfigs = Map(ClusterLinkConfig.AclSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.AclFiltersProp -> aclJson)
    )
    createClusterLinks(args = Array("--acl-filters-json", aclJson,"--validate-only"),
      additionalConfigs = Map(ClusterLinkConfig.AclSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.AclFiltersProp -> aclJson),
      expectValidateOnly = true
    )
    createClusterLinks(args = Array("--acl-filters-json", aclJson,"--exclude-validate-link"),
      additionalConfigs = Map(ClusterLinkConfig.AclSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.AclFiltersProp -> aclJson),
      expectValidateLink = false
    )
  }

  @Test
  def testCreateClusterLinksWithOffsetAndAclSyncConfigs(): Unit = {
    createClusterLinks(args = Array("--acl-filters-json", aclJson,"--consumer-group-filters-json",offsetJson),
      additionalConfigs = Map(ClusterLinkConfig.AclSyncEnableProp -> "true",ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.AclFiltersProp -> aclJson,ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetJson)
    )
    createClusterLinks(args = Array("--acl-filters-json", aclJson,"--consumer-group-filters-json",offsetJson,"--validate-only"),
      additionalConfigs = Map(ClusterLinkConfig.AclSyncEnableProp -> "true",ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.AclFiltersProp -> aclJson,ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetJson),
      expectValidateOnly = true
    )
    createClusterLinks(args = Array("--acl-filters-json", aclJson,"--consumer-group-filters-json",offsetJson,"--exclude-validate-link"),
      additionalConfigs = Map(ClusterLinkConfig.AclSyncEnableProp -> "true",ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true"),
      expectedConfigs = Map(ClusterLinkConfig.AclFiltersProp -> aclJson,ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetJson),
      expectValidateLink = false
    )
  }

  @Test
  def testCreateClusterLinks(): Unit = {
    createClusterLinks(Array.empty)
    createClusterLinks(Array("--validate-only"), expectValidateOnly = true)
    createClusterLinks(Array("--exclude-validate-link"), expectValidateLink = false)
  }

  private def listClusterLinks(args: Array[String], hasLinkName: Boolean, includeTopics: Boolean): Unit = {
    var issuedCommand = false

    def toTopics(topics: Option[Set[String]]) = topics match {
      case Some(t) => Optional.of(t.asJavaCollection)
      case None => Optional.empty[Collection[String]]
    }
    val (topics1, topics2) = if (includeTopics)
      (toTopics(Some(Set("topic-1", "topic-2", "topic-3"))), toTopics(Some(Set("topic-7", "topic-8", "topic-9"))))
    else
      (toTopics(None), toTopics(None))

    val link1 = new ClusterLinkListing("link-1", UUID.randomUUID(), "cluster-id-1", topics1)
    val link2 = new ClusterLinkListing("link-2", UUID.randomUUID(), null, topics2)
    val listing = if (hasLinkName) Set(link1) else Set(link1, link2)

    val future = new KafkaFutureImpl[Collection[ClusterLinkListing]]
    future.complete(listing.asJava)

    val result: ListClusterLinksResult = EasyMock.createNiceMock(classOf[ListClusterLinksResult])
    EasyMock.expect(result.result()).andReturn(future.asInstanceOf[KafkaFuture[Collection[ClusterLinkListing]]]).once()

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new TestAdminClient(node) {
      override def listClusterLinks(options: ListClusterLinksOptions): ListClusterLinksResult = {
        issuedCommand = true
        if (hasLinkName) {
          assertTrue(options.linkNames.isPresent)
          assertEquals(Set(link1.linkName), options.linkNames.get.asScala.toSet)
        } else {
          assertFalse(options.linkNames.isPresent)
        }
        assertEquals(includeTopics, options.includeTopics)
        result
      }
    }

    EasyMock.replay(result)
    val output = runCommand(Array("--list") ++ args, mockAdminClient)
    assertTrue(issuedCommand)
    EasyMock.reset(result)

    listing.foreach { entry =>
      assertTrue(output.contains(entry.linkName))
      assertTrue(output.contains(entry.linkId.toString))
      if (entry.clusterId != null)
        assertTrue(output.contains(entry.clusterId))
      if (includeTopics)
        entry.topics.get.asScala.foreach(t => assertTrue(output.contains(t)))
    }
  }

  @Test
  def testListClusterLinks(): Unit = {
    listClusterLinks(Array.empty, hasLinkName = false, includeTopics = false)
    listClusterLinks(Array("--include-topics"), hasLinkName = false, includeTopics = true)
    listClusterLinks(Array("--link-name", "link-1"), hasLinkName = true, includeTopics = false)
    listClusterLinks(Array("--link-name", "link-1", "--include-topics"), hasLinkName = true, includeTopics = true)
  }

  private def deleteClusterLinks(args: Array[String], expectValidateOnly: Boolean = false, expectForce: Boolean = false): Unit = {
    val linkName = "test-link"
    var issuedCommand = false

    val future = new KafkaFutureImpl[Void]
    future.complete(null)

    val result: DeleteClusterLinksResult = EasyMock.createNiceMock(classOf[DeleteClusterLinksResult])
    EasyMock.expect(result.all()).andReturn(future.asInstanceOf[KafkaFuture[Void]]).once()

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new TestAdminClient(node) {
      override def deleteClusterLinks(linkNames: Collection[String], options: DeleteClusterLinksOptions): DeleteClusterLinksResult = {
        issuedCommand = true
        assertEquals(1, linkNames.size)
        assertEquals(linkName, linkNames.iterator.next)
        assertEquals(expectValidateOnly, options.validateOnly)
        assertEquals(expectForce, options.force)
        result
      }
    }

    EasyMock.replay(result)
    val output = runCommand(Array("--delete", "--link-name", linkName) ++ args, mockAdminClient)
    assertTrue(issuedCommand)
    EasyMock.reset(result)

    val expectedOutput = if (expectValidateOnly) validated else completed
    assertTrue(output.contains(expectedOutput))
  }

  @Test
  def testDeleteClusterLinks(): Unit = {
    deleteClusterLinks(Array.empty)
    deleteClusterLinks(Array("--validate-only"), expectValidateOnly = true)
    deleteClusterLinks(Array("--force"), expectForce = true)
  }

  @Test
  def testBadCommands(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092")).verifyArgs()
    }
    intercept[OptionException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--not-a-command")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--create", "--list")).verifyArgs()
    }
  }

  @Test
  def testBadConfigs(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--create", "--link-name", "test-link")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array(
        "--create", "--link-name", "test-link",
        "--config", "bootstrap.servers=10.20.30.40:9092",
        "--config-file", "cluster-link.properties")).verifyArgs()
    }
  }

  @Test
  def testMissingRequiredArgs(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--list")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "--config", "bootstrap.servers=10.20.30.40")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "--link-name", "test-link")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "test-link", "--exclude-validate-link")).verifyArgs()
    }
  }

  @Test
  def testInvalidArgs(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "--link-name", "test-link", "--config", "bootstrap.servers=10.20.30.40:9092", "--force")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--list", "--force")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--list", "--cluster-id", "123")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--list",
        "--config", "bootstrap.servers=10.20.30.40:9092")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete", "--link-name", "test-link", "--cluster-id", "123")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete", "--link-name", "test-link", "--config", "bootstrap.servers=10.20.30.40:9092")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete", "--link-name", "test-link", "--exclude-validate-link")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "--link-name", "test-link", "--config", "bootstrap.servers=10.20.30.40:9092", "--include-topics")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete", "--link-name", "test-link", "--include-topics")).verifyArgs()
    }
  }

  class TestAdminClient(node: Node) extends MockAdminClient(Collections.singletonList(node), node) {
    override def createClusterLinks(clusterLinks: Collection[NewClusterLink], options: CreateClusterLinksOptions): CreateClusterLinksResult =
      EasyMock.createNiceMock(classOf[CreateClusterLinksResult])
    override def listClusterLinks(options: ListClusterLinksOptions): ListClusterLinksResult =
      EasyMock.createNiceMock(classOf[ListClusterLinksResult])
    override def deleteClusterLinks(linkNames: Collection[String], options: DeleteClusterLinksOptions): DeleteClusterLinksResult =
      EasyMock.createNiceMock(classOf[DeleteClusterLinksResult])
  }
}
