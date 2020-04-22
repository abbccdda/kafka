/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.admin

import java.util.Properties
import java.util.concurrent.ExecutionException
import joptsimple.ArgumentAcceptingOptionSpec

import kafka.common.AdminCommandFailedException
import kafka.utils.CommandDefaultOptions
import kafka.utils.CommandLineUtils
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ConfluentAdmin, KafkaAdminClient, ListTopicsOptions, ReplicaStatusOptions}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.replica.ReplicaStatus
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.collection.Seq

object ReplicaStatusCommand extends Logging {
  private val allColumns = List("Topic", "Partition", "Replica", "IsLeader", "IsObserver", "IsIsrEligible", "IsInIsr", "IsCaughtUp",
    "LastCaughtUpLagMs", "LastFetchLagMs", "LogStartOffset", "LogEndOffset")

  case class Args(topics: Seq[String],
    partitions: Seq[Int],
    leaders: Option[Boolean],
    observers: Option[Boolean],
    verboseOutput: Boolean,
    jsonOutput: Boolean,
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
    AdminClient.create(props).asInstanceOf[KafkaAdminClient]
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

    val entries = mutable.ListBuffer[List[String]]()
    result.result.asScala.toList.sortBy { case (key, _) =>
      (key.topic, key.partition)
    }.foreach { case (partition, replicas) =>
      val status = replicas.get.asScala.sortBy(_.brokerId)
      val leaderTimeMs = status.filter(_.isLeader).map(_.lastCaughtUpTimeMs).headOption.getOrElse[Long](0)
      status.filterNot(st => args.leaders.exists(_ != st.isLeader) || args.observers.exists(_ != st.isObserver))
        .map(entries += toEntries(args, partition, _, leaderTimeMs))
    }

    if (args.jsonOutput)
      printJson(entries.toList)
    else if (args.verboseOutput)
      printVerbose(entries.toList)
    else
      printCompact(args, entries.toList)
  }

  private def toEntries(args: Args, partition: TopicPartition, status: ReplicaStatus, leaderTimeMs: Long): List[String] = {

    def toLagMsStr(timeMs: Long): String = if (timeMs > 0) (leaderTimeMs - timeMs).toString else "-1"

    def toLogOffsetStr(logOffset: Long): String = if (logOffset >= 0) logOffset.toString else "-1"

    List(
      partition.topic,
      partition.partition.toString,
      status.brokerId.toString,
      status.isLeader.toString,
      status.isObserver.toString,
      status.isIsrEligible.toString,
      status.isInIsr.toString,
      status.isCaughtUp.toString,
      toLagMsStr(status.lastCaughtUpTimeMs),
      toLagMsStr(status.lastFetchTimeMs),
      toLogOffsetStr(status.logStartOffset),
      toLogOffsetStr(status.logEndOffset)
    )
  }

  private def printJson(entries: List[List[String]]): Unit = {
    if (entries.isEmpty) {
      println("[]")
      return
    }

    def addQuotes(string: String) = s""""${string}""""

    var lastTopic = ""
    var lastPartition = ""

    println("[")

    def closeTopic(): Unit = {
      println
      println("    ]")
      print("  }")
    }

    def closePartition(): Unit = {
      println
      println("        ]")
      print("      }")
    }

    entries.zipWithIndex.foreach { case (entry, index) =>
      val newTopic = lastTopic != entry(0)
      val newPartition = newTopic || lastPartition != entry(1)

      if (index > 0) {
        if (newPartition)
          closePartition
        if (newTopic)
          closeTopic
      }
      if (newTopic) {
        if (index > 0)
          println(",")
        println("  {")
        println(s"""    "Topic": ${addQuotes(entry(0))},""")
        println("""    "Partitions": [""")
      }
      if (newPartition) {
        if (!newTopic)
          println(",")
        println("      {")
        println(s"""        "Partition": ${entry(1)},""")
        println("""        "Replicas": [""")
      }

      lastTopic = entry(0)
      lastPartition = entry(1)

      if (!newPartition)
        println(",")
      println("          {")
      entry.zipWithIndex.foreach { case(subEntry, subIndex) =>
        if (subIndex >= 2) {
          print(s"            ${addQuotes(allColumns(subIndex))}: ${subEntry}")
          if (subIndex < entry.size - 1)
            println(",")
          else
            println
        }
      }
      print("          }")
    }

    closePartition
    closeTopic
    println
    println("]")
  }

  private def printVerbose(entries: List[List[String]]): Unit = {
    entries.zipWithIndex.foreach { case (entry, index) =>
      allColumns.zip(entry).foreach { subEntry =>
        println(s"${subEntry._1}: ${subEntry._2}")
      }
      if (index < entries.size - 1)
        println
    }
  }

  private def printCompact(args: Args, entries: List[List[String]]): Unit = {
    val maxWidth = mutable.ArrayBuffer[Int]()
    allColumns.zipWithIndex.foreach { case (name, index) =>
      maxWidth += name.size
    }
    entries.foreach { entry =>
      entry.zipWithIndex.foreach { case (subEntry, index) =>
        maxWidth(index) = maxWidth(index).max(subEntry.size)
      }
    }

    def printEntry(string: String, index: Int): Unit = {
      print(string)
      print(" " * (maxWidth(index) - string.size + 1))
    }
    def printEntries(entries: List[String]): Unit = {
      entries.zipWithIndex.foreach(e => printEntry(e._1, e._2))
      println
    }

    printEntries(allColumns)
    entries.foreach(printEntries(_))
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

    val verboseOutputArg = commandOptions.options.has(commandOptions.verboseOutputOpt)
    val jsonOutputArg = commandOptions.options.has(commandOptions.jsonOutputOpt)
    val excludeInternalTopicsArg = commandOptions.options.has(commandOptions.excludeInternalTopicsOpt)

    def parseBoolOpt(spec: ArgumentAcceptingOptionSpec[String]): Option[Boolean] = {
      if (!commandOptions.options.has(spec))
        None
      else if (commandOptions.options.valueOf(spec) == null)
        Some(true)
      else commandOptions.options.valueOf(spec).toLowerCase match {
        case "only" => Some(true)
        case "exclude" => Some(false)
        case value => throw new IllegalArgumentException(s"Unexpected value: ${value}")
      }
    }
    val leadersArg = parseBoolOpt(commandOptions.leadersOpt)
    val observersArg = parseBoolOpt(commandOptions.observersOpt)

    Args(topicsArg, partitionsArg, leadersArg, observersArg, verboseOutputArg, jsonOutputArg, excludeInternalTopicsArg)
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
  val leadersOpt = parser
    .accepts("leaders", "If set, only display the partition leaders' status information, or excluded if 'exclude'.")
    .withOptionalArg
    .describedAs("leaders")
    .ofType(classOf[String])
  val observersOpt = parser
    .accepts("observers", "If set, only display the partition observers' status information, or excluded if 'exclude'.")
    .withOptionalArg
    .describedAs("observers")
    .ofType(classOf[String])
  val verboseOutputOpt = parser
    .accepts("verbose", "If set, show verbose output with one attribute per line.")
  val jsonOutputOpt = parser
    .accepts("json", "If set, show output in JSON format.")
  val excludeInternalTopicsOpt = parser
    .accepts("exclude-internal", "If set, exclude internal topics. All topics will be displayed by default.")

  options = parser.parse(args: _*)
}
