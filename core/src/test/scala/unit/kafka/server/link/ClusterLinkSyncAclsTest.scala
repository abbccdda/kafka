/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.Collections._
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.{Collections, Optional, Properties}

import kafka.controller.KafkaController
import kafka.security.authorizer.AclEntry
import kafka.utils.Implicits._
import org.apache.kafka.clients.admin.{ConfluentAdmin, DescribeAclsResult}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType._
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.{AuthenticationException, AuthorizationException}
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult
import org.apache.kafka.server.authorizer.{AclCreateResult, AclDeleteResult, Authorizer}
import org.easymock.EasyMock._
import org.easymock.{EasyMock => EM}
import org.junit.{After, Before, Test}

import scala.jdk.CollectionConverters._

class ClusterLinkSyncAclsTest {

  private val scheduler = new ClusterLinkScheduler()
  private val admin: ConfluentAdmin = createNiceMock(classOf[ConfluentAdmin])
  private val clientManager: ClusterLinkClientManager = createNiceMock(classOf[ClusterLinkClientManager])
  private val authorizer: Authorizer = createNiceMock(classOf[Authorizer])
  private val controller: KafkaController = createNiceMock(classOf[KafkaController])
  private val aclList: util.List[AclBinding] = new util.ArrayList[AclBinding]()
  val migrateAllAclsJson: String =
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
  val multipleAclFiltersJson: String =
    """{
      | "aclFilters": [{
      |  "resourceFilter": {
      |      "resourceType": "topic",
      |      "name": "foo",
      |      "patternType": "literal"
      |    },
      |  "accessFilter": {
      |     "principal": "User:Bob",
      |     "host":"*",
      |     "operation": "read",
      |     "permissionType": "allow"
      |    }
      |  },
      |  {
      |  "resourceFilter": {
      |      "resourceType": "topic",
      |      "name": "foo",
      |      "patternType": "prefixed"
      |    },
      |  "accessFilter": {
      |     "principal": "User:Alice",
      |     "host":"*",
      |     "operation": "alter",
      |     "permissionType": "allow"
      |    }
      |  },
      |  {
      |  "resourceFilter": {
      |      "resourceType": "cluster",
      |      "name": "*",
      |      "patternType": "literal"
      |    },
      |  "accessFilter": {
      |     "principal": "User:Mallory",
      |     "host":"badhost",
      |     "operation": "clusterAction",
      |     "permissionType": "deny"
      |    }
      |  }]
      | }""".stripMargin


  @Before
  def setUp(): Unit = {
    scheduler.startup()
  }

  @After
  def tearDown(): Unit = {
    scheduler.shutdown()
    aclList.clear()
  }

  private def setupMock(): Unit = {
    reset(admin)
    reset(clientManager)
    reset(authorizer)
    reset(controller)

    expect(clientManager.scheduler).andReturn(scheduler).anyTimes()
    expect(clientManager.getAdmin).andReturn(admin).anyTimes()
  }

  @Test
  def testAclAddition(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(1)
    replay(controller)

    addAclBinding(aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW))
    addAclBinding(aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW))
    val acls = addAclBinding(aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY))
    val describeAclsResult = new DescribeAclsResult (KafkaFuture.completedFuture(acls))
    expect(admin.describeAcls(AclBindingFilter.ANY)).andReturn(describeAclsResult).times(1)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(1)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(3)).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> migrateAllAclsJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = describeAclsResult.values().get().asScala.toSet
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testAclAdditionWithMultipleAclBindingFilters(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(1)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter

    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(describeAclsResult1).times(1)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(1)
    expect(admin.describeAcls(aclBindingFilter3)).andReturn(describeAclsResult3).times(1)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(1)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(3)).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = Set(aclBinding1, aclBinding2, aclBinding3)
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testNoRepeatedUpdateWhenNoChange(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    addAclBinding(aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW))
    addAclBinding(aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW))
    val acls = addAclBinding(aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY))
    val describeAclsResult = new DescribeAclsResult (KafkaFuture.completedFuture(acls))
    expect(admin.describeAcls(AclBindingFilter.ANY)).andReturn(describeAclsResult).times(2)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(3)).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> migrateAllAclsJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = describeAclsResult.values().get().asScala.toSet
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testNoRepeatedUpdateWhenNoChangeWithMultipleAclBindingFilters(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter

    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(describeAclsResult1).times(2)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(2)
    expect(admin.describeAcls(aclBindingFilter3)).andReturn(describeAclsResult3).times(2)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(3)).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = Set(aclBinding1, aclBinding2, aclBinding3)
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testAclDeletion(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    // first adding acls
    val aclToDelete = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    addAclBinding(aclToDelete)
    addAclBinding(aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW))
    val acls1 = new util.ArrayList[AclBinding](addAclBinding(aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)))
    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(acls1))
    // deleting acls, removing User:Bob's ACLs
    val acls2 = removeAclBinding(aclToDelete)
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(acls2))
    expect(admin.describeAcls(AclBindingFilter.ANY))
      .andReturn(describeAclsResult1).times(1)
      .andReturn(describeAclsResult2).times(1)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(3)).times(1)

    // removing User:Bob's ACLs
    val deletedAclBinding = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    expect(authorizer.deleteAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclDeleteResult]]])
      .andReturn(aclDeletionSuccessResults(Collections.singletonList(deletedAclBinding))).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> migrateAllAclsJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = describeAclsResult2.values().get().asScala.toSet
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testAclDeletionWithMultipleAclBindingFilters(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter

    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    val deletedDescribeAclsResult = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.emptyList()))
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(describeAclsResult1).times(1)
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(deletedDescribeAclsResult).times(1)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(2)
    expect(admin.describeAcls(aclBindingFilter3)).andReturn(describeAclsResult3).times(2)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(3)).times(1)
    expect(authorizer.deleteAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclDeleteResult]]])
      .andReturn(aclDeletionSuccessResults(Collections.singletonList(aclBinding1))).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = Set(aclBinding2, aclBinding3)
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testAclAdditionAndDeletion(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    // first adding ACLs
    val aclToDelete = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    addAclBinding(aclToDelete)
    val acls1 = new util.ArrayList[AclBinding](addAclBinding(aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)))
    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(acls1))
    // deleting and adding ACLs, removing User:Bob's ACLs and adding User:Mallory's ACLs
    val addedAcl = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)
    addAclBinding(addedAcl)
    val acls2 = removeAclBinding(aclToDelete)
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(acls2))
    expect(admin.describeAcls(AclBindingFilter.ANY))
      .andReturn(describeAclsResult1).times(1)
      .andReturn(describeAclsResult2).times(1)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)


    // first add User:Alice's and User:Bob's ACLs
    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(2)).times(1)

    // adding User:Mallory's ACLs
    val createAclList2 = Collections.singletonList(addedAcl)
    expect(authorizer.createAcls(null, createAclList2, Optional.empty())
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(1)).times(1)

    // removing User:Bob's ACLs
    val deletedAclBinding = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val deleteAclList = singletonList(deletedAclBinding.toFilter)
    expect(authorizer.deleteAcls(null, deleteAclList, Optional.empty())
      .asInstanceOf[util.List[CompletableFuture[AclDeleteResult]]])
      .andReturn(aclDeletionSuccessResults(Collections.singletonList(deletedAclBinding))).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> migrateAllAclsJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
    val expectedAclSet = describeAclsResult2.values().get().asScala.toSet
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testAclAdditionAndDeletionWithMultipleAclBindingFilters(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter
    // deleting and adding ACLs, removing User:Bob's ACLs and adding User:Mallory's ACLs
    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    val emptyDescribeAclsResult = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.emptyList()))
    expect(admin.describeAcls(aclBindingFilter1))
      .andReturn(describeAclsResult1).times(1)
      .andReturn(emptyDescribeAclsResult).times(1)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(2)
    expect(admin.describeAcls(aclBindingFilter3))
      .andReturn(emptyDescribeAclsResult).times(1)
      .andReturn(describeAclsResult3).times(1)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)

    val aclList3 = Collections.singletonList(aclBinding3)
    // first add User:Alice's and User:Bob's ACLs
    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(2)).times(1)
    // removing User:Bob's ACLs
    expect(authorizer.deleteAcls(null, Collections.singletonList(aclBinding1.toFilter),
      Optional.empty()).asInstanceOf[util.List[CompletableFuture[AclDeleteResult]]])
      .andReturn(aclDeletionSuccessResults(Collections.singletonList(aclBinding1))).times(1)
    // adding User:Mallory's ACLs
    expect(authorizer.createAcls(null, aclList3, Optional.empty())
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(1)).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = Set(aclBinding2, aclBinding3)
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }


  @Test
  def testNoExceptionsThrownIfUnableToGetSourceAcls(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(1)
    replay(controller)

    expect(admin.describeAcls(AclBindingFilter.ANY)).andThrow(new AuthorizationException("Unauthorized for DESCRIBE on Cluster"))
    replay(admin)

    replay(clientManager)

    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> migrateAllAclsJson))
    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    assert(syncAclsTask.getCurrentAclSet.isEmpty)
  }

  @Test
  def testErrorOnAclAdditionUpdatesLocalAclSetCorrectly(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(1)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter

    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(describeAclsResult1).times(1)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(1)
    expect(admin.describeAcls(aclBindingFilter3)).andReturn(describeAclsResult3).times(1)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(1)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andThrow(new AuthorizationException("Unable to create ACLs")).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    assert(Set() == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testErrorOnAclDeletionUpdatesLocalAclSetCorrectly(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter

    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    val deletedDescribeAclsResult = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.emptyList()))
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(describeAclsResult1).times(1)
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(deletedDescribeAclsResult).times(1)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(2)
    expect(admin.describeAcls(aclBindingFilter3)).andReturn(describeAclsResult3).times(2)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(2)).times(1)
    expect(authorizer.deleteAcls(null, Collections.singletonList(aclBinding1.toFilter),
      Optional.empty()).asInstanceOf[util.List[CompletableFuture[AclDeleteResult]]])
      .andThrow(new AuthorizationException("Unable to delete ACLs")).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = Set(aclBinding1, aclBinding2, aclBinding3)
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testErrorOnAclAdditionFutureUpdatesLocalAclSetCorrectly(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(1)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter

    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(describeAclsResult1).times(1)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(1)
    expect(admin.describeAcls(aclBindingFilter3)).andReturn(describeAclsResult3).times(1)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(1)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationFailureResults(3)).times(1)
    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    assert(Set() == syncAclsTask.getCurrentAclSet)
  }

  @Test
  def testErrorOnAclDeletionFutureUpdatesLocalAclMapCorrectly(): Unit = {
    setupMock()

    expect(controller.isActive).andReturn(true).times(2)
    replay(controller)

    val aclBinding1 = aclBinding(TOPIC, "foo", LITERAL, "User:Bob",
      AclEntry.WildcardHost, READ, ALLOW)
    val aclBinding2 = aclBinding(TOPIC, "foo", PREFIXED, "User:Alice",
      AclEntry.WildcardHost, ALTER, ALLOW)
    val aclBinding3 = aclBinding(CLUSTER, ResourcePattern.WILDCARD_RESOURCE, LITERAL,
      "User:Mallory", "badhost", CLUSTER_ACTION, DENY)

    val aclBindingFilter1 = aclBinding1.toFilter
    val aclBindingFilter2 = aclBinding2.toFilter
    val aclBindingFilter3 = aclBinding3.toFilter

    val describeAclsResult1 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding1)))
    val describeAclsResult2 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding2)))
    val describeAclsResult3 = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.singletonList(aclBinding3)))
    val deletedDescribeAclsResult = new DescribeAclsResult (KafkaFuture.completedFuture(Collections.emptyList()))
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(describeAclsResult1).times(1)
    expect(admin.describeAcls(aclBindingFilter1)).andReturn(deletedDescribeAclsResult).times(1)
    expect(admin.describeAcls(aclBindingFilter2)).andReturn(describeAclsResult2).times(2)
    expect(admin.describeAcls(aclBindingFilter3)).andReturn(describeAclsResult3).times(2)
    replay(admin)

    expect(clientManager.getAuthorizer).andReturn(Some(authorizer)).times(2)
    replay(clientManager)

    expect(authorizer.createAcls(EM.eq(null), notNull(), EM.eq(Optional.empty()))
      .asInstanceOf[util.List[CompletableFuture[AclCreateResult]]])
      .andReturn(aclCreationSuccessResults(3)).times(1)
    expect(authorizer.deleteAcls(null, Collections.singletonList(aclBinding1.toFilter),
      Optional.empty()).asInstanceOf[util.List[CompletableFuture[AclDeleteResult]]])
      .andReturn(aclDeletionFailureResults(Collections.singletonList(aclBinding1))).times(1)

    replay(authorizer)

    val config = newConfig(Map(ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> multipleAclFiltersJson))

    val syncAclsTask = new ClusterLinkSyncAcls(clientManager, config, controller)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)
    syncAclsTask.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()

    val expectedAclSet = Set(aclBinding1, aclBinding2, aclBinding3)
    assert(expectedAclSet == syncAclsTask.getCurrentAclSet)
  }


  private def aclBinding(resourceType: ResourceType, resourceName: String,
                       patternType: PatternType, principalName: String, hostName: String,
                       aclOperation: AclOperation, permissionType: AclPermissionType)
  : AclBinding = {
    val resourcePattern: ResourcePattern = new ResourcePattern(resourceType, resourceName,
      patternType)
    val accessControlEntry: AccessControlEntry = new AccessControlEntry(
      principalName, hostName, aclOperation, permissionType)
    new AclBinding(resourcePattern, accessControlEntry)
  }

  private def addAclBinding(acl: AclBinding): util.List[AclBinding] = {
    aclList.add(acl)
    aclList
  }

  private def removeAclBinding(acl: AclBinding): util.List[AclBinding] = {
    aclList.remove(acl)
    aclList
  }

  private def aclCreationSuccessResults(num: Int): util.List[CompletableFuture[AclCreateResult]] = {
    val successList: util.List[CompletableFuture[AclCreateResult]] = new util.ArrayList[CompletableFuture[AclCreateResult]]()
    for (_ <- 0 until num) {
      successList.add(CompletableFuture.completedFuture(AclCreateResult.SUCCESS))
    }
    successList
  }

  private def aclCreationFailureResults(num: Int): util.List[CompletableFuture[AclCreateResult]] = {
    val failureList: util.List[CompletableFuture[AclCreateResult]] = new util.ArrayList[CompletableFuture[AclCreateResult]]()
    for (_ <- 0 until num) {
      failureList.add(CompletableFuture.completedFuture(new AclCreateResult(
        new AuthenticationException("Unable to authenticate"))))
    }
    failureList
  }

  private def aclDeletionSuccessResults(deletedAcls: util.List[AclBinding]): util.List[CompletableFuture[AclDeleteResult]] = {
    val aclBindingDeleteResult: util.Collection[AclBindingDeleteResult] = new util.ArrayList[AclBindingDeleteResult]()
    for (acl <- deletedAcls.asScala) {
      val aclBinding = new AclBindingDeleteResult(acl)
      aclBindingDeleteResult.add(aclBinding)
    }
    Collections.singletonList(CompletableFuture.completedFuture(new AclDeleteResult(aclBindingDeleteResult)))
  }

  private def aclDeletionFailureResults(deletedAcls: util.List[AclBinding]): util.List[CompletableFuture[AclDeleteResult]] = {
    val aclBindingDeleteResult: util.Collection[AclBindingDeleteResult] = new util.ArrayList[AclBindingDeleteResult]()
    for (acl <- deletedAcls.asScala) {
      val aclBinding = new AclBindingDeleteResult(acl,
        new AuthenticationException("Unable to authenticate"))
      aclBindingDeleteResult.add(aclBinding)
    }
    Collections.singletonList(CompletableFuture.completedFuture(new AclDeleteResult(aclBindingDeleteResult)))
  }

  private def newConfig(configs: Map[String, String]): ClusterLinkConfig = {
    val props = new Properties()
    props ++= configs
    new ClusterLinkConfig(props)
  }

  private def verifyMock(): Unit = {
    verify(clientManager)
    verify(controller)
    verify(admin)
    verify(authorizer)
  }
}
