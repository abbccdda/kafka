/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.admin

import java.util.Properties
import java.util.concurrent.ExecutionException
import kafka.common.AdminCommandFailedException
import kafka.utils.CommandDefaultOptions
import kafka.utils.CommandLineUtils
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{AdminClientConfig, ConfluentAdmin, ListTopicsOptions, ReplicaStatusOptions}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.replica.ReplicaStatus
import org.apache.kafka.common.utils.Utils
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._

object ReplicaStatusCommand extends Logging {
  case class Args(topics: Seq[String],
    partitions: Seq[Int],
    modes: Set[ReplicaStatus.Mode],
    compactOutput: Boolean,
    onlyNotCaughtUp: Boolean,
    excludeInternalTopics: Boolean)

  def main(args: Array[String]): Unit = {
    run(args, 30.second)
  }

  def run(args: Array[String], timeout: Duration): Unit = {
    val commandOptions = new ReplicaStatusCommandOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(commandOptions, "This tool prints out the replica status of partitions.")

    val initializedArgs = validateAndInitializeArgs(commandOptions)
    val adminClient = createAdminClient(commandOptions, timeout)
    try {
      replicaStatus(initializedArgs, adminClient)
    } finally {
      adminClient.close()
    }
  }

  private def createAdminClient(commandOptions: ReplicaStatusCommandOptions, timeout: Duration): ConfluentAdmin = {
    val props = if (commandOptions.options.has(commandOptions.adminClientConfigOpt))
      Utils.loadProps(commandOptions.options.valueOf(commandOptions.adminClientConfigOpt))
    else
      new Properties()

    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, commandOptions.options.valueOf(commandOptions.bootstrapServer))
    props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout.toMillis.toString)
    ConfluentAdmin.create(props)
  }

  private def replicaStatus(args: Args, client: ConfluentAdmin): Unit = {
    try {
      doReplicaStatus(args, client)
    } catch {
      case e: ExecutionException =>
        e.getCause match {
          case cause: TimeoutException =>
            val message = "Timeout waiting for results"
            println(message)
            throw new AdminCommandFailedException(message, cause)
          case cause: ClusterAuthorizationException =>
            val message = "Not authorized"
            println(message)
            throw new AdminCommandFailedException(message, cause)
          case _ =>
            throw e
        }
      case e: Throwable =>
        println("Error while issuing request")
        throw e
    }
  }

  private def doReplicaStatus(args: Args, client: ConfluentAdmin): Unit = {
    val topics = if (args.topics != null)
      args.topics.filterNot(Topic.isInternal(_) && args.excludeInternalTopics)
    else {
      debug(s"Calling AdminClient.listTopics()")
      client.listTopics(new ListTopicsOptions().listInternal(!args.excludeInternalTopics)).names().get().asScala.toSeq
    }

    val topicPartitions = if (args.partitions != null) {
      topics.flatMap(topic => args.partitions.map(new TopicPartition(topic, _)))
    } else {
      debug(s"Calling AdminClient.describeTopics(${topics}")
      client.describeTopics(topics.asJavaCollection).all().get().values().asScala.flatMap { td =>
        td.partitions.asScala.map(pi => new TopicPartition(td.name, pi.partition))
      }
    }

    debug(s"Calling AdminClient.replicaStatus(${topicPartitions})")
    val result = client.replicaStatus(topicPartitions.toSet.asJava, new ReplicaStatusOptions)

    if (args.compactOutput)
      println("Topic\tPartition\tReplica\tMode\tIsCaughtUp\tIsInSync\tLastCaughtUpLagMs\tLastFetchLagMs\tLogStartOffset\tLogEndOffset")

    result.result.asScala.toList.sortBy { case (key, _) =>
      (key.topic, key.partition)
    }.foreach { case (partition, replicas) =>
      val status = replicas.get.asScala.sortBy(_.brokerId)
      val leaderTimeMs = status.filter(_.mode == ReplicaStatus.Mode.LEADER).map(_.lastCaughtUpTimeMs).headOption.getOrElse[Long](0)
      status.foreach(maybePrintReplicaStatus(args, partition, _, leaderTimeMs))
    }
  }

  private def maybePrintReplicaStatus(args: Args, partition: TopicPartition, status: ReplicaStatus, leaderTimeMs: Long): Unit = {
    if (args.onlyNotCaughtUp && status.isCaughtUp)
      return
    if (!args.modes.contains(status.mode))
      return

    def toYesNo(yes: Boolean): String = if (yes) "yes" else "no"

    def toLagMsStr(timeMs: Long): String = if (timeMs <= 0) "unknown" else (leaderTimeMs - timeMs).toString

    def toLogOffsetStr(logOffset: Long): String = if (logOffset < 0) "unknown" else logOffset.toString

    def printEntry(key: String, value: String, first: Boolean, last: Boolean): Unit = if (args.compactOutput) {
      if (!first) print("\t")
      print(value)
      if (last) println()
    } else {
      if (!first) print("\t")
      println(s"${key}: ${value}")
    }

    if (args.compactOutput) {
      printEntry("Topic", partition.topic, true, false)
      printEntry("Partition", partition.partition.toString, false, false)
      printEntry("Replica", status.brokerId.toString, false, false)
    } else {
      printEntry("Topic-Partition-Replica", s"${partition.topic}-${partition.partition}-${status.brokerId}", true, false)
    }
    printEntry("Mode", status.mode.toString, false, false)
    printEntry("IsCaughtUp", toYesNo(status.isCaughtUp), false, false)
    printEntry("IsInSync", toYesNo(status.isInSync), false, false)
    printEntry("LastCaughtUpLagMs", toLagMsStr(status.lastCaughtUpTimeMs), false, false)
    printEntry("LastFetchLagMs", toLagMsStr(status.lastFetchTimeMs), false, false)
    printEntry("LogStartOffset", toLogOffsetStr(status.logStartOffset), false, false)
    printEntry("LogEndOffset", toLogOffsetStr(status.logEndOffset), false, true)
  }

  private def validateAndInitializeArgs(commandOptions: ReplicaStatusCommandOptions): Args = {
    // Required options: --bootstrap-server
    var missingOptions = List.empty[String]
    if (!commandOptions.options.has(commandOptions.bootstrapServer)) {
      missingOptions = commandOptions.bootstrapServer.options().get(0) :: missingOptions
    }
    if (missingOptions.nonEmpty) {
      throw new AdminCommandFailedException(s"Missing required option(s): ${missingOptions.mkString(", ")}")
    }

    // Extract the topics, if provided.
    var topicsArg: Seq[String] = null
    if (commandOptions.options.has(commandOptions.topicsOpt)) {
      topicsArg = commandOptions.options.valueOf(commandOptions.topicsOpt).split(",")
      topicsArg.foreach(Topic.validate(_))
    }

    // Extract the partitions, if provided.
    var partitionsArg: Seq[Int] = null
    if (commandOptions.options.has(commandOptions.partitionsOpt)) {
      val tmpPartitions = new mutable.ListBuffer[Int]
      commandOptions.options.valueOf(commandOptions.partitionsOpt).split(",").foreach { partitionStr =>
        val divPartitions = partitionStr.split("-")
        if (divPartitions.isEmpty || divPartitions.size > 2)
          throw new IllegalArgumentException(s"Invalid partition range: ${partitionStr}")
        try {
          val first = divPartitions(0).toInt
          if (divPartitions.size == 1)
            tmpPartitions += first
          else {
            val second = divPartitions(1).toInt
            if (first > second)
              throw new IllegalArgumentException(s"Invalid partition range: ${partitionStr}")
            for (i <- first until second + 1)
              tmpPartitions += i
          }
        } catch {
          case e: NumberFormatException =>
            throw new IllegalArgumentException(s"Failed to parse partition: ${partitionStr}")
        }
      }
      partitionsArg = tmpPartitions;
    }

    // Extract the modes, if provided, otherwise initialize the modes to include all.
    val modesArg = if (commandOptions.options.has(commandOptions.modesOpt)) {
      commandOptions.options.valueOf(commandOptions.modesOpt).split(",").map { value =>
        try {
          ReplicaStatus.Mode.valueOf(value.toUpperCase)
        } catch {
          case e: IllegalArgumentException =>
            throw new IllegalArgumentException(s"Failed to parse mode: ${value}")
        }
      }.toSet
    } else
      ReplicaStatus.Mode.values().toSet

    val compactOutputArg = commandOptions.options.has(commandOptions.compactOutputOpt);
    val onlyNotCaughtUpArg = commandOptions.options.has(commandOptions.onlyNotCaughtUpOpt);
    val excludeInternalTopicsArg = commandOptions.options.has(commandOptions.excludeInternalTopicsOpt);

    Args(topicsArg, partitionsArg, modesArg, compactOutputArg, onlyNotCaughtUpArg, excludeInternalTopicsArg)
  }
}

private final class ReplicaStatusCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
  val bootstrapServer = parser
    .accepts("bootstrap-server",
      "A hostname and port for the broker to connect to, in the form host:port. Multiple comma separated URLs can be given. REQUIRED.")
    .withRequiredArg
    .describedAs("host:port")
    .ofType(classOf[String])
  val adminClientConfigOpt = parser
    .accepts("admin.config", "Configuration properties files to pass to the admin client.")
    .withRequiredArg
    .describedAs("config file")
    .ofType(classOf[String])
  val topicsOpt = parser
    .accepts("topics", "Comma-separated topics to retrieve replica status for.")
    .withRequiredArg
    .describedAs("topics")
    .ofType(classOf[String])
  val partitionsOpt = parser
    .accepts("partitions", "Comma-separated list of partition IDs or ID ranges for the topic(s), e.g. \"5,10-20\".")
    .withRequiredArg
    .describedAs("partitions")
    .ofType(classOf[String])
  val modesOpt = parser
    .accepts("modes", "The modes for the displayed replicas. If empty, all replicas will be displayed.")
    .withRequiredArg
    .describedAs("leader,follower,observer")
    .ofType(classOf[String])
  val compactOutputOpt = parser
    .accepts("compact", "If set, show compact output with one replica per line, values separated by spaces.")
  val onlyNotCaughtUpOpt = parser
    .accepts("only-not-caught-up", "If set, only show replicas that aren't caught up. All replicas are displayed by default.")
  val excludeInternalTopicsOpt = parser
    .accepts("exclude-internal", "If set, exclude internal topics. All topics will be displayed by default.")

  options = parser.parse(args: _*)
}
