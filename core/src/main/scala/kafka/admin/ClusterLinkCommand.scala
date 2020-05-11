/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.admin

import java.util.Properties
import java.util.concurrent.ExecutionException

import joptsimple._
import kafka.common.AdminCommandFailedException
import kafka.server.link.{AclJson, ClusterLinkConfig}
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Logging}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.errors.{ClusterAuthorizationException, TimeoutException}
import org.apache.kafka.common.requests.NewClusterLink
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.jdk.CollectionConverters._

object ClusterLinkCommand extends Logging {

  def main(args: Array[String]): Unit = {
    var exitCode = 0;
    try {
      run(args)
    } catch {
      case e: Throwable =>
        println("Error while executing cluster link command: " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      Exit.exit(exitCode)
    }
  }

  private def createAdminClient(opts: ClusterLinkCommandOptions): ConfluentAdmin = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()

    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServer))
    AdminClient.create(props).asInstanceOf[ConfluentAdmin]
  }

  private def loadConfigs(opts: ClusterLinkCommandOptions): Map[String, String] = {
    val props = if (opts.options.has(opts.configOpt)) {
      AdminUtils.parseConfigs(opts.valueOf(opts.configOpt))
    } else {
      require(opts.options.has(opts.configFileOpt))
      Utils.loadProps(opts.options.valueOf(opts.configFileOpt))
    }

    if (props.getProperty(ClusterLinkConfig.AclSyncEnableProp, "false").equals("true")) {
      var jsonString = ""
      if (opts.options.has(opts.aclFiltersJsonFileOpt)) {
        jsonString = Utils.readFileAsString(opts.valueOf(opts.aclFiltersJsonFileOpt))
      } else if (opts.options.has(opts.aclFiltersJsonOpt)) {
        jsonString = opts.valueOf(opts.aclFiltersJsonOpt)
      }
      if (jsonString.trim.isEmpty) {
        CommandLineUtils.printHelpAndExitIfNeeded(opts, s"${ClusterLinkConfig.AclSyncEnableProp}" +
          s" is set to true but the acl filters JSON is not passed in. Please pass in the path to" +
          s" the JSON file using the --acl-filters-json-file option and rerun the create link command.")
      }
      val json = AclJson.parse(jsonString)
      json match {
        case Some(_) => props.put(ClusterLinkConfig.AclFiltersProp, jsonString)
        case None =>
          CommandLineUtils.printHelpAndExitIfNeeded(opts,
            s"${ClusterLinkConfig.AclSyncEnableProp} is set to true but the JSON file passed"
              + s" has invalid values. Please put valid values in the JSON file and rerun the" +
              s" create link command.")
      }
    }

    props.asScala.toMap
  }

  def run(args: Array[String], clientOpt: Option[ConfluentAdmin] = None): Unit = {
    val opts = new ClusterLinkCommandOptions(args)
    opts.checkArgs()

    val client = clientOpt.getOrElse(createAdminClient(opts))

    try {
      if (opts.options.has(opts.createOpt))
        createClusterLink(opts, client)
      else if (opts.options.has(opts.listOpt))
        listClusterLinks(opts, client)
      else if (opts.options.has(opts.deleteOpt))
        deleteClusterLink(opts, client)
    } catch {
      case e: ExecutionException =>
        def throwAdminCommandFailedException(message: String, cause: Throwable): Unit = {
          println(message)
          throw new AdminCommandFailedException(message, cause)
        }
        e.getCause match {
          case cause: TimeoutException =>
            throwAdminCommandFailedException("Timed out waiting for results", e)
          case cause: ClusterAuthorizationException =>
            throwAdminCommandFailedException("Not authorized", e)
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

  private def createClusterLink(opts: ClusterLinkCommandOptions, client: ConfluentAdmin): Unit = {
    val linkName = opts.valueOf(opts.linkNameOpt)
    val clusterId = opts.valueAsOption(opts.clusterIdOpt)
    val validateOnly = opts.options.has(opts.validateOnlyOpt)
    val excludeValidateLink = opts.options.has(opts.excludeValidateLinkOpt)

    val clusterLink = new NewClusterLink(linkName, clusterId.orNull, loadConfigs(opts).asJava)
    val options = new CreateClusterLinksOptions().validateOnly(validateOnly).validateLink(!excludeValidateLink)
    client.createClusterLinks(Seq(clusterLink).asJava, options).all().get()

    val action = if (validateOnly) "validated" else "completed"
    println(s"Cluster link '$linkName' creation successfully $action.")
  }

  private def listClusterLinks(opts: ClusterLinkCommandOptions, client: ConfluentAdmin): Unit = {
    val options = new ListClusterLinksOptions()
    val result = client.listClusterLinks(options).result().get().asScala
    if (result.isEmpty)
      println("No cluster links found.")
    else
      result.foreach {
        case cl => println(s"Link name: '${cl.linkName}', link ID: '${cl.linkId}', cluster ID: '${cl.clusterId}'")
      }
  }

  private def deleteClusterLink(opts: ClusterLinkCommandOptions, client: ConfluentAdmin): Unit = {
    val linkName = opts.valueOf(opts.linkNameOpt)
    val validateOnly = opts.options.has(opts.validateOnlyOpt)
    val force = opts.options.has(opts.forceOpt)

    val options = new DeleteClusterLinksOptions().validateOnly(validateOnly).force(force)
    client.deleteClusterLinks(Seq(linkName).asJava, options).all().get()

    val action = if (validateOnly) "validated" else "completed"
    println(s"Cluster link '$linkName' deletion successfully $action.")
  }
}

private final class ClusterLinkCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
  private val nl = System.getProperty("line.separator")

  val bootstrapServer = parser.accepts("bootstrap-server",
    "A hostname and port for the broker to connect to, in the form host:port. Multiple comma separated URLs can be given. REQUIRED.")
    .withRequiredArg
    .describedAs("host:port")
    .ofType(classOf[String])
  val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
    .withRequiredArg
    .describedAs("file")
    .ofType(classOf[String])
  val createOpt = parser.accepts("create", "Creates a new cluster link.")
  val listOpt = parser.accepts("list", "Lists all available cluster links.")
  val deleteOpt = parser.accepts("delete", "Deletes a cluster link.")
  val linkNameOpt = parser.accepts("link-name", "The name for the cluster link.")
    .withRequiredArg
    .describedAs("link name")
    .ofType(classOf[String])
  val clusterIdOpt = parser.accepts("cluster-id", "The required cluster ID for the linked cluster.")
    .withRequiredArg
    .describedAs("cluster ID")
    .ofType(classOf[String])
  val aclFiltersJsonFileOpt = parser.accepts("acl-filters-json-file", ClusterLinkConfig.AclFiltersDoc)
    .withRequiredArg
    .describedAs("path to ACL filters JSON file")
    .ofType(classOf[String])
  val aclFiltersJsonOpt = parser.accepts("acl-filters-json", ClusterLinkConfig.AclFiltersDoc)
    .withRequiredArg
    .describedAs("JSON of ACL filters")
    .ofType(classOf[String])
  val configOpt = parser.accepts("config",
    "A cluster link configuration for the cluster link being created. The following is a list of valid configurations: " +
      nl + ClusterLinkConfig.configNames.map("\t" + _).mkString(nl) + nl +
      "See the Kafka documentation for full details on the cluster link configs.")
    .withRequiredArg
    .describedAs("key=value,...")
    .ofType(classOf[String])
  val configFileOpt = parser.accepts("config-file", "Property file containing configs to be used for the created cluster link.")
    .withRequiredArg
    .describedAs("file")
    .ofType(classOf[String])
  val forceOpt = parser.accepts("force", "When deleting a link, force its deletion even if there's outstanding references (e.g. topic mirrors) to the link.")
  val validateOnlyOpt = parser.accepts("validate-only", "Whether to only validate the action but not apply it.")
  val excludeValidateLinkOpt = parser.accepts("exclude-validate-link", "If set, then when creating a link, do not attempt to validate the link with the link's cluster.")

  options = parser.parse(args: _*)

  def valueOf[T](opt: OptionSpec[T]): T = options.valueOf(opt)

  def valueAsOption[T](opt: OptionSpec[T]): Option[T] = if (options.has(opt))
    Some(options.valueOf(opt))
  else
    None

  private def verifyRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*): Unit = {
    for (arg <- required) {
      if (!options.has(arg))
        throw new IllegalArgumentException(s"Missing required argument '$arg'")
    }
  }

  private def verifyInvalidArgs(parser: OptionParser, options: OptionSet, usedOption: OptionSpec[_], invalidOptions: Set[OptionSpec[_]]): Unit = {
    if (options.has(usedOption)) {
      for (arg <- invalidOptions) {
        if (options.has(arg))
          throw new IllegalArgumentException(s"Option '$usedOption' can't be used with option '$arg'")
      }
    }
  }

  def verifyArgs(): Unit = {
    verifyRequiredArgs(parser, options, bootstrapServer)

    if (Seq(createOpt, listOpt, deleteOpt).count(options.has) != 1)
      throw new IllegalArgumentException("Command must include exactly one action: --create, --list, or --delete.")

    if (options.has(createOpt)) {
      if (Seq(configOpt, configFileOpt).count(options.has) != 1)
        throw new IllegalArgumentException("Command must include required configs for cluster link creation.")
      if (options.has(excludeValidateLinkOpt) && !options.has(clusterIdOpt))
        throw new IllegalArgumentException("Command must include --cluster-id if --exclude-validate-link is specified.")
    }

    if (options.has(createOpt) || options.has(deleteOpt))
      verifyRequiredArgs(parser, options, linkNameOpt)

    verifyInvalidArgs(parser, options, createOpt, Set(forceOpt))
    verifyInvalidArgs(parser, options, listOpt, Set(linkNameOpt, clusterIdOpt, configOpt, configFileOpt, forceOpt, excludeValidateLinkOpt))
    verifyInvalidArgs(parser, options, deleteOpt, Set(clusterIdOpt, configOpt, configFileOpt, excludeValidateLinkOpt))
  }

  def checkArgs(): Unit = {
    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Create, list, or delete cluster links.")
    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool creates, lists, and deletes cluster links.")
    CommandLineUtils.checkInvalidArgs(parser, options, aclFiltersJsonFileOpt, Set(listOpt, deleteOpt, forceOpt))
    CommandLineUtils.checkInvalidArgs(parser, options, aclFiltersJsonOpt, Set(listOpt, deleteOpt, forceOpt))
    try {
      verifyArgs()
    } catch {
      case e: Throwable => CommandLineUtils.printUsageAndDie(parser, e.getMessage())
    }
  }
}
