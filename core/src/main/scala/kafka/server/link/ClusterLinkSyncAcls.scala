/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture

import kafka.controller.KafkaController
import org.apache.kafka.clients.admin.DescribeAclsResult
import org.apache.kafka.common.{KafkaFuture, MetricName}
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.metrics.{Metrics, Sensor}
import org.apache.kafka.common.metrics.stats.{Rate, CumulativeSum}
import org.apache.kafka.server.authorizer.{AclCreateResult, AclDeleteResult}

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ClusterLinkSyncAcls (val clientManager: ClusterLinkClientManager,
                           controller: KafkaController, metrics: Metrics,
                           metricsTags: java.util.Map[String, String])
  extends ClusterLinkScheduler.PeriodicTask(clientManager.scheduler, name = "SyncAcls",
    clientManager.currentConfig.aclSyncMs) {

  // set of last seen AclBindings from source cluster
  private val currentAclSet = mutable.Set[AclBinding]()

  // integer counting number of tasks outstanding for acl migration
  private var tasksOutstanding = 0
  private var aclsAddedSensor: Sensor = _
  private var aclsDeletedSensor: Sensor = _

  override def startup(): Unit = {
    aclsAddedSensor = metrics.sensor("acls-added-sensor")
    val aclsAddedTotal = new MetricName("acls-added-total",
      "cluster-link-metrics", "Total number of ACLs added.", metricsTags)
    val aclsAddedRate = new MetricName("acls-added-rate",
      "cluster-link-metrics", "Rate of ACLs added.", metricsTags)
    aclsAddedSensor.add(aclsAddedTotal, new CumulativeSum)
    aclsAddedSensor.add(aclsAddedRate, new Rate)

    aclsDeletedSensor = metrics.sensor("acls-deleted-sensor")
    val aclsDeletedTotal = new MetricName("acls-deleted-total",
      "cluster-link-metrics", "Total number of ACLs deleted.", metricsTags)
    val aclsDeletedRate = new MetricName("acls-deleted-rate",
      "cluster-link-metrics", "Rate of ACLs deleted.", metricsTags)
    aclsDeletedSensor.add(aclsDeletedTotal, new CumulativeSum)
    aclsDeletedSensor.add(aclsDeletedRate, new Rate)

    super.startup()
  }

  override def shutdown(): Unit = {
    metrics.removeSensor("acls-added-sensor")
    metrics.removeSensor("acls-deleted-sensor")
    super.shutdown()
  }

  /**
   * Starts running the task, returning whether the task has completed.
   *
   * This method calls describeAcls on the source cluster and attempts to retrieve all the
   * necessary ACLs based on the passed in AclBindingFilters parsed from the acl filter JSON.
   * Every time a list of AclBindings have been retrieved from the source cluster, we call
   * updateAcls() to update the local cache of the currentAclSet and check to see if we need to
   * create or delete any ACLs on the destination cluster, and then proceed to do so.
   *
   * @return `true` if the task has completed, otherwise `false` if there's outstanding work to
   *        be done
   */
  override protected def run(): Boolean = {
    if (controller.isActive) {
      val aclFilterJson = clientManager.currentConfig.aclFilters.get
      val aclFilterList = AclJson.toAclBindingFilters(aclFilterJson)
      val describeAclsResultList = ListBuffer[Option[DescribeAclsResult]]()
      for (aclFilter <- aclFilterList) {
        val describeAclsResult = try {
          trace("Attempting to retrieve ACLs from source cluster...")
          Some(clientManager.getAdmin.describeAcls(aclFilter))
        } catch {
          case _: AuthorizationException =>
            warn("Unable to retrieve ACLs on source cluster. Please enable DESCRIBE ACLs on"
              + " the source cluster to proceed with ACL migration.")
            None
          case e: Throwable =>
            warn("Unexpected error encountered while trying to retrieve ACLs on source"
              + " cluster: "+ e)
            None
        }
        if (describeAclsResult.isDefined) {
          describeAclsResultList += describeAclsResult
        }
      }
      if (describeAclsResultList.nonEmpty) {
        val futureList: ListBuffer[KafkaFuture[util.Collection[AclBinding]]] =
          describeAclsResultList.map(result => result.get.values())
        val future: KafkaFuture[Void] = KafkaFuture.allOf(futureList.toArray: _*)
        scheduleWhenComplete(future, () => updateAcls(futureList))
        tasksOutstanding += 1
      }
    }
    tasksOutstanding == 0
  }

  /**
   * This method takes in the set of AclBindings retrieved from the source cluster and keeps track
   * of the set of created and deleted ACLs. Based on these created and deleted sets, it goes ahead
   * and calls methods to update the current ACL set based on the computed added/deleted acls.
   */
  private def updateAcls(futureList: ListBuffer[KafkaFuture[util.Collection[AclBinding]]]):
  Boolean = {
    val describeAclResultSet = mutable.Set[AclBinding]()
    futureList.foreach { future => describeAclResultSet ++= future.get().asScala }
    val deletedAcls = currentAclSet.diff(describeAclResultSet)
    val addedAcls = describeAclResultSet.diff(currentAclSet)
    clientManager.getAuthorizer.foreach { auth =>
      if (addedAcls.nonEmpty) {
        try {
          val addedAclsList = addedAcls.toList
          val createdAclResults = auth.createAcls(null,
            addedAclsList.asJava, Optional.empty()).asScala.map(_.toCompletableFuture)
          val createdAclsFuture = CompletableFuture.allOf(createdAclResults.toArray: _*)
          scheduleWhenComplete(createdAclsFuture, () =>
            addAclsAndLogCreationWarnings(createdAclResults.toList, addedAclsList, addedAcls))
          tasksOutstanding += 1
          aclsAddedSensor.record(addedAcls.size)
        } catch {
          case e: Throwable =>
            warn("Unexpected error encountered while trying to create ACLs on destination"
              + " cluster: "+ e)
        }
      }
      if (deletedAcls.nonEmpty) {
        try {
          val deleteAclsFilterList = (for (acl <- deletedAcls) yield acl.toFilter).toList
          val deletedAclResults = auth.deleteAcls(null,
            deleteAclsFilterList.asJava, Optional.empty()).asScala.map(_.toCompletableFuture)
          val deletedAclsFuture = CompletableFuture.allOf(deletedAclResults.toArray: _*)
          scheduleWhenComplete(deletedAclsFuture, () =>
            deleteAclsAndLogDeletionWarnings(deletedAclResults.toList, deletedAcls,
              deleteAclsFilterList))
          tasksOutstanding += 1
          aclsDeletedSensor.record(deletedAcls.size)
        } catch {
          case e: Throwable =>
            warn("Unexpected error encountered while trying to create ACLs on destination"
              + " cluster: "+ e)
        }
      }
    }
    tasksOutstanding -= 1
    tasksOutstanding == 0
  }

  /**
   * This method logs any exceptions encountered while attempting to create ACLs and also ensures
   * to remove any ACLs from the added acl set that weren't able to be created on the destination
   * cluster due to errors encountered. Finally, it updates the current acl set with the added acls.
   */
  private def addAclsAndLogCreationWarnings(aclCreateResultList: List[CompletableFuture[AclCreateResult]],
                                            createdAclList: List[AclBinding],
                                            addedAclSet: mutable.Set[AclBinding]): Boolean = {
    val addedAcls = addedAclSet
    aclCreateResultList.zip(createdAclList).foreach { case (future, createdList) =>
      try {
        val createdAcl = future.get()
        if (createdAcl.exception.isPresent) {
          warn("Encountered the following exception while trying to create ACL: " +
            createdAcl.exception.get())
          // If an error was encountered while we tried to create the ACL, then we should assume
          // that the ACL didn't get created and we must remove it from the added ACL set so that
          // we try again on the next refresh. Since the list of results are returned in the order
          // we called them, we can assume the size of both lists are the same and there is an exact
          // correlation between the indices. Therefore we can use i to update the added acl set.
          addedAcls -= createdList
        }
      } catch {
        case e: Throwable =>
          warn("Unexpected error encountered while trying to create ACL: "+ e)
          // If an error was encountered while trying to get the future of AclCreateResult,
          // then we should assume that the ACL didn't get created and we must remove it from the
          // added ACL set so that we try again on the next refresh. Since the list of results are
          // returned in the order we called them, we can assume the size of both lists are the same
          // and there is an exact correlation between the indices. Therefore we can use i to update
          // the added acl set. While we don't except the future to throw an error, this is here
          // just in case as to not disrupt the cluster link.
          addedAcls -= createdList
      }
    }
    currentAclSet ++= addedAcls
    tasksOutstanding -= 1
    tasksOutstanding == 0
  }

  /**
   * This method logs any exceptions encountered while attempting to delete ACLs and also ensures
   * to remove any ACLs from the deleted acl set that weren't able to be deleted on the destination
   * cluster due to errors encountered. Finally, it updates the current acl set with the deleted
   * acls.
   */
  private def deleteAclsAndLogDeletionWarnings(aclDeleteResultList: List[CompletableFuture[AclDeleteResult]],
                                               deletedAclSet: mutable.Set[AclBinding],
                                               deletedAclFilterList: List[AclBindingFilter]): Boolean = {
    val deletedAcls = deletedAclSet
    aclDeleteResultList.zip(deletedAclFilterList).foreach { case(future, filter) =>
      try{
        val aclDeleteResult: AclDeleteResult = future.get()
        val aclBindingDeleteResultList: util.Collection[AclDeleteResult.AclBindingDeleteResult]
        = aclDeleteResult.aclBindingDeleteResults()
        for (aclBindingDeleteResult <- aclBindingDeleteResultList.asScala) {
          if (aclBindingDeleteResult.exception().isPresent) {
            warn("Encountered the following exception while trying to delete ACL: " +
              aclBindingDeleteResult.exception.get())
            // If an error was encountered while we tried to delete the ACL, then we should assume
            // that the ACL didn't get deleted and we must remove it from our deleted acls set so
            // that we try again on the next refresh.
            deletedAcls -= aclBindingDeleteResult.aclBinding()
          }
        }
      } catch {
        case e: Throwable =>
          warn("Unexpected error encountered while trying to delete ACL: "+ e)
          // If an error was encountered while trying to get the future of a specific
          // AclDeleteResult, then we have to make sure that all acls in the deleted acl set that
          // match that specific acl filter must be deleted. The results for each filter
          // returned from deleteAcls are in the same order as the input filter list, therefore we
          // can use i in this case to grab the corresponding filter and then delete all AclBindings
          // match that filter. While we don't except the future to throw an error, this is here
          // just in case so we don't disrupt the cluster link.
          deletedAcls --= deletedAcls.filter(filter.matches)
      }
    }
    currentAclSet --= deletedAcls
    tasksOutstanding -= 1
    tasksOutstanding == 0
  }

  // for testing purposes
  def getCurrentAclSet: mutable.Set[AclBinding] = currentAclSet
}
