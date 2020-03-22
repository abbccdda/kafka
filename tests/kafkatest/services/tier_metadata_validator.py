import os

from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.kafka import config_property

"""
The tier storage metadata validator is a service which validates metadata integrity. Currently it downloads the tier
state files for each kafka broker, runs materialization validation against them before moving to another node.
Next enhancement will be to run for all nodes at once and be rolling/periodical run until stopped.
"""


class TierMetadataValidator(KafkaPathResolverMixin, JmxMixin, BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/tier_metadata_validation"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "tier_metadata_validation.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "tier_metadata_validation.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    WORK_DIR = os.path.join(PERSISTENT_ROOT, "working-dir")
    SNAPSHOT_DIR = os.path.join(WORK_DIR, "snapshots")
    LOG_FILE = os.path.join(LOG_DIR, "tier_storage_metadata_validation.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "tier_metadata_validation.properties")
    JMX_TOOL_LOG = os.path.join(PERSISTENT_ROOT, "jmx_tool.log")
    JMX_TOOL_ERROR_LOG = os.path.join(PERSISTENT_ROOT, "jmx_tool.err.log")
    TIER_STATE_FILE_PREFIX = "tierstate"
    BOOTSERVER_PORT = "9092"

    logs = {
        "validator_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "validator_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "validator_log": {
            "path": LOG_FILE,
            "collect_default": True},
        "jmx_log": {
            "path": JMX_TOOL_LOG,
            "collect_default": False},
        "jmx_err_log": {
            "path": JMX_TOOL_ERROR_LOG,
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, metadata_states_dir, jmx_object_names=None, jmx_attributes=None,
                 offset_scan=False):
        """
        Args:
            context:                    standard context
            num_nodes:                  number of nodes to use (this should be 1)
            kafka:                      kafka service
            metadata_states_dir         states
        """
        JmxMixin.__init__(self, num_nodes=num_nodes, jmx_object_names=jmx_object_names,
                          jmx_attributes=(jmx_attributes or []),
                          root=TierMetadataValidator.PERSISTENT_ROOT)
        BackgroundThreadService.__init__(self, context, num_nodes)
        self.kafka = kafka
        self.metadata_states_dir = metadata_states_dir
        self.bootstrap_server = self.kafka.nodes[0].account.hostname + ":" + TierMetadataValidator.BOOTSERVER_PORT
        self.jaas_override_variables = {}
        self.offset_scan = offset_scan
        self.broker_id = self.kafka.idx(self.kafka.nodes[0])

    def start_cmd(self):
        """Return the start command appropriate for the given node."""
        args = {'metadata_states_dir': self.metadata_states_dir,
                'bootstrap_server': self.bootstrap_server,
                'tier_storage_metadata_validator': self.path.script(
                    "kafka-run-class.sh kafka.tier.tools.TierMetadataValidator"),
                'stdout': TierMetadataValidator.STDOUT_CAPTURE,
                'stderr': TierMetadataValidator.STDERR_CAPTURE,
                'log_dir': TierMetadataValidator.LOG_DIR,
                'log4j_config': TierMetadataValidator.LOG4J_CONFIG,
                'config_file': TierMetadataValidator.CONFIG_FILE,
                'jmx_port': self.jmx_port,
                'kafka_opts': self.security_config.kafka_opts,
                'working_dir': TierMetadataValidator.WORK_DIR}

        cmd = "export JMX_PORT=%(jmx_port)s; " \
              "export LOG_DIR=%(log_dir)s; " \
              "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j_config)s\"; " \
              "export KAFKA_OPTS=%(kafka_opts)s; " \
              "%(tier_storage_metadata_validator)s " \
              "--bootstrap-server=%(bootstrap_server)s " \
              "--snapshot-states-files=false " \
              "--working-dir=%(working_dir)s " % args

        tier_config = dict(self.kafka.server_prop_overides)
        if tier_config[config_property.CONFLUENT_TIER_BACKEND] \
                and not tier_config[config_property.CONFLUENT_TIER_BACKEND] == "S3":
            raise ValueError("Unsupported backend: %s" % tier_config[config_property.CONFLUENT_TIER_BACKEND])
        elif tier_config[config_property.CONFLUENT_TIER_BACKEND]:
            self.logger.debug(
                "Configuring tiered storage with backend: %s" % tier_config[config_property.CONFLUENT_TIER_BACKEND])
            cmd = "%s " \
                  "--validate-tier-storage=true " \
                  "--cluster-id=kafka " \
                  "--%s=%d " \
                  "--%s=%s " \
                  "--%s=%s " \
                  "--%s=%s " % (cmd,
                                config_property.BROKER_ID,
                                self.broker_id,
                                config_property.CONFLUENT_TIER_BACKEND,
                                tier_config[config_property.CONFLUENT_TIER_BACKEND],
                                config_property.CONFLUENT_TIER_S3_BUCKET,
                                tier_config[config_property.CONFLUENT_TIER_S3_BUCKET],
                                config_property.CONFLUENT_TIER_S3_REGION,
                                tier_config[config_property.CONFLUENT_TIER_S3_REGION])
            if self.offset_scan:
                cmd = "%s --validate-tier-storage-offset=true " % cmd

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s &" % args
        return cmd

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def _worker(self, idx, node):
        self.security_config = self.kafka.security_config.client_config(node=node,
                                                                        jaas_override_variables=self.jaas_override_variables)
        self.security_config.setup_node(node)

        node.account.ssh("mkdir -p %s" % TierMetadataValidator.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=TierMetadataValidator.LOG_FILE)
        node.account.create_file(TierMetadataValidator.LOG4J_CONFIG, log_config)

        ret_status = 0

        for knode in self.kafka.nodes:
            # Create working directory
            node.account.ssh("mkdir -p %s" % TierMetadataValidator.WORK_DIR, allow_fail=False)
            node.account.ssh("mkdir -p %s" % TierMetadataValidator.SNAPSHOT_DIR, allow_fail=False)

            # Search and load all the states file ending with *.tier
            files = knode.account.ssh_capture(
                "find %s -name '*.%s'" % (self.metadata_states_dir, TierMetadataValidator.TIER_STATE_FILE_PREFIX))
            self.logger.info("Found following tier-state files %s", files)

            for state_file in files:
                state_file = state_file.strip('\n')
                dir_name = os.path.dirname(state_file)
                file_name = os.path.basename(state_file)
                dest_dir = os.path.join(TierMetadataValidator.SNAPSHOT_DIR, dir_name.split("/")[-1].strip())
                dst_file = os.path.join(dest_dir, file_name)
                self.logger.info("Copying %s:%s to %s:%s", knode.account.hostname, state_file, node.account.hostname,
                                 dest_dir)
                node.account.ssh("mkdir -p %s" % dest_dir, allow_fail=False)
                knode.account.copy_between(state_file, dst_file, node)

                if not node.account.isfile(os.path.join(dest_dir, file_name)):
                    self.logger.error("Failed to copy %s on %s", dst_file, node.account.hostname)
                else:
                    self.logger.info("Copied state file %s", dst_file)

            self.logger.debug("Running validation on %s", node.account.hostname)
            # Run and capture output
            cmd = self.start_cmd()
            output = node.account.ssh_capture(cmd, allow_fail=False)
            for line in output:
                self.logger.info(line)

            self.logger.debug("Completed tier storage metadata validator %s command: ", knode.account.hostname)

            # Cleanup
            with self.lock:
                self.logger.debug("collecting following jmx objects: %s", self.jmx_object_names)
                self.start_jmx_tool(idx, node)
                node.account.ssh("rm -rf %s" % TierMetadataValidator.SNAPSHOT_DIR, allow_fail=False)
                node.account.ssh("rm -rf %s" % TierMetadataValidator.WORK_DIR, allow_fail=False)

        return ret_status

    def stop_node(self, node):
        self.logger.info("%s Stopping node %s" % (self.__class__.__name__, str(node.account)))
        node.account.kill_java_processes(self.java_class_name(),
                                         clean_shutdown=True, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=30)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(30))

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        JmxMixin.clean_node(self, node)
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf %s" % TierMetadataValidator.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def java_class_name(self):
        return "TierMetadataValidator"
