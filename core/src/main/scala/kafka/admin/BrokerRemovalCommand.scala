/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.admin

import java.util.Properties

import joptsimple._
import kafka.common.AdminCommandFailedException
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Logging}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.errors.{ApiException, BrokerNotAvailableException, BrokerRemovalInProgressException, BrokerRemovedException, ClusterAuthorizationException, InvalidRequestException, TimeoutException}
import org.apache.kafka.common.utils.{Exit, Utils}

object BrokerRemovalCommand extends Logging {

  def main(args: Array[String]): Unit = {
    var exitCode = 0
    try {
      run(args)
    } catch {
      case e: Throwable =>
        println("Error while executing broker removal: " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      Exit.exit(exitCode)
    }
  }

  private def createAdminClient(opts: BrokerRemovalCommandOptions): ConfluentAdmin = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()

    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServer))
    ConfluentAdmin.create(props)
  }

  def run(args: Array[String], clientOpt: Option[ConfluentAdmin] = None): Unit = {
    val opts = new BrokerRemovalCommandOptions(args)
    opts.checkArgs()

    val client = clientOpt.getOrElse(createAdminClient(opts))

    try {
      if (opts.options.has(opts.deleteOpt))
        removeBroker(opts, client)
      else if (opts.options.has(opts.describeOpt))
        describeBrokerRemoval(client)
    } catch {
      case e: ApiException =>
        e match {
          case _: TimeoutException =>
            throw new AdminCommandFailedException("Timed out waiting for results", e)
          case cause: ClusterAuthorizationException =>
            throw new AdminCommandFailedException(cause.getMessage, e)
          case cause: InvalidRequestException =>
            throw new AdminCommandFailedException(cause.getMessage, e)
          case cause: BrokerNotAvailableException =>
            throw new AdminCommandFailedException(cause.getMessage, e)
          case _: BrokerRemovalInProgressException =>
            throw new AdminCommandFailedException(s"Broker ${opts.valueOf(opts.brokerIdOpt)} is already being removed", e)
          case _: BrokerRemovedException =>
            throw new AdminCommandFailedException(s"Broker ${opts.valueOf(opts.brokerIdOpt)} has already been removed", e)
          case _ =>
            throw e
        }
      case e: Throwable =>
        println("Error while issuing request")
        throw e
    } finally {
      client.close()
    }
  }

  private def removeBroker(opts: BrokerRemovalCommandOptions, client: ConfluentAdmin): Unit = {
    val brokerId = opts.valueOf(opts.brokerIdOpt)
    val brokerIds = new java.util.ArrayList[Integer]()
    brokerIds.add(brokerId.toInt)
    println("Initiating remove broker call...")
    client.removeBrokers(brokerIds).all().get()
    println(s"Started remove broker task for broker $brokerId.")
    println(s"You can check its status by calling this command again with the `--describe` option.")
  }

  private def describeBrokerRemoval(client: ConfluentAdmin): Unit = {
    val result = client.describeBrokerRemovals().descriptions().get()
    if (result.isEmpty) {
      println("No broker removals in progress.")
    } else {
      result forEach((id, desc) => println(
        s"Broker $id removal status:\n  " +
          s"Partition Reassignment: ${desc.partitionReassignmentsStatus()}\n  " +
          s"Broker Shutdown: ${desc.brokerShutdownStatus()}"))
    }
  }
}

private final class BrokerRemovalCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {

  val bootstrapServer: ArgumentAcceptingOptionSpec[String] = parser.accepts("bootstrap-server",
    "A hostname and port for the broker to connect to, in the form host:port. Multiple comma separated URLs can be given. REQUIRED.")
    .withRequiredArg
    .describedAs("host:port")
    .ofType(classOf[String])
  val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
    .withRequiredArg
    .describedAs("file")
    .ofType(classOf[String])
  val deleteOpt: OptionSpecBuilder = parser.accepts("delete", "Delete one broker from the cluster.")
  val describeOpt: OptionSpecBuilder = parser.accepts("describe", "Describe the status of broker removal.")
  val brokerIdOpt: ArgumentAcceptingOptionSpec[String] = parser.accepts("broker-id", "The id of the Kafka broker to be removed/being removed. REQUIRED.")
    .withRequiredArg
    .describedAs("broker id")
    .ofType(classOf[String])

  options = parser.parse(args: _*)

  def valueOf[T](opt: OptionSpec[T]): T = options.valueOf(opt)

  private def verifyRequiredArgs(options: OptionSet, required: OptionSpec[_]*): Unit = {
    for (arg <- required) {
      if (!options.has(arg))
        throw new IllegalArgumentException(s"Missing required argument '$arg'")
    }
  }

  def verifyArgs(): Unit = {
    verifyRequiredArgs(options, bootstrapServer, brokerIdOpt)

    if (Seq(deleteOpt, describeOpt).count(options.has) != 1)
      throw new IllegalArgumentException("Command must include exactly one action: --delete or --describe.")
  }

  def checkArgs(): Unit = {
    if (args.length == 0) {
      CommandLineUtils.printUsageAndDie(parser, "Remove Kafka broker and describe the status of the removal.")
    }
    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool remove Kafka broker and describe the status of the removal.")
    try {
      verifyArgs()
    } catch {
      case e: Throwable => CommandLineUtils.printUsageAndDie(parser, e.getMessage)
    }
  }
}
