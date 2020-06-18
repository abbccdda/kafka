import os

from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import config_property
from ducktape.utils.util import wait_until
from kafkatest.utils.tiered_storage import TierSupport
import uuid
import json
import base64
import time

# TierFenceRestore is implemented as a background service as it copies tier state files
# from all brokers, compares and uploads them to object storage. Helpful logs are captured
# for this service in the TierFenceRestore directory in the test results directory.
# These can be used to debug failures in the recovery process.
class TierFenceRestore(KafkaPathResolverMixin, TierSupport, BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/tier_fence_restore"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "tier_metadata_validation.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "tier_metadata_validation.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    COMPARATOR_OUTPUT = os.path.join(PERSISTENT_ROOT, "comparator.json")
    RESTORE_OUTPUT = os.path.join(PERSISTENT_ROOT, "restore.json")
    FENCE_OUTPUT = os.path.join(PERSISTENT_ROOT, "fence.json")
    WORK_DIR = os.path.join(PERSISTENT_ROOT, "working-dir")
    SNAPSHOT_DIR = os.path.join(WORK_DIR, "snapshots")
    LOG_FILE = os.path.join(LOG_DIR, "tier_storage_metadata_validation.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "tier_metadata_validation.properties")
    TIER_STATE_FILE_SUFFIX = "tierstate"
    BOOTSERVER_PORT = "9092"

    logs = {
        "validator_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "validator_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "validator_log": {
            "path": LOG_FILE,
            "collect_default": True},
        "comparator_output": {
            "path": COMPARATOR_OUTPUT,
            "collect_default": True},
        "fence_output": {
            "path": FENCE_OUTPUT,
            "collect_default": True},
        "restore_output": {
            "path": RESTORE_OUTPUT,
            "collect_default": True}
    }

    def __init__(self, context, kafka, topic, partition_ids_to_be_fenced, should_restore):
        """
        Args:
            context: standard context
            kafka: kafka service
            topic: the topic to fence and restore
            partition_ids_to_be_fenced: partition ids to be fenced and restored
            should_restore: boolean denoting whether to restore the fenced state. 
                            This is parameterized to allow for correct behavior under fencing to be tested.
        """
        BackgroundThreadService.__init__(self, context, 1)
        self.kafka = kafka
        self.metadata_states_dir = kafka.DATA_LOG_DIR_1
        self.bootstrap_server = self.kafka.nodes[0].account.hostname + ":" + TierFenceRestore.BOOTSERVER_PORT
        self.should_restore = should_restore
        self.jaas_override_variables = {}
        self.topic = topic
        self.partition_ids_to_be_fenced = partition_ids_to_be_fenced
        self.broker_id = self.kafka.idx(self.kafka.nodes[0])

    def topic_id(self, partition):
        self.logger.debug(
            "Querying zookeeper to find assigned topic ID for topic %s and partition %d" % (self.topic, partition))
        zk_path = "/brokers/topics/%s" % self.topic
        topic_info_json = self.kafka.zk.query(zk_path, chroot=self.kafka.zk_chroot)

        if topic_info_json is None:
            raise Exception("Error finding state for topic %s (partition %d)." % (self.topic, partition))

        topic_info = json.loads(topic_info_json)
        self.logger.info(topic_info)
        topic_id = topic_info["confluent_topic_id"]
        self.logger.info("Topic ID assigned for topic %s is %s (partition %d)" % (self.topic, topic_id, partition))
        return topic_id

    def fence_and_restore(self, node, should_restore):
        """fences and then restores tier-states.
        Args:
            node: tier_fence_restore node
            should_restore: boolean denoting whether to restore the fenced state. 
                            This is parameterized to allow for correct behavior under fencing to be tested.
            """
        args = {'metadata_states_dir': self.metadata_states_dir,
                'bootstrap_server': self.bootstrap_server,
                'stdout': TierFenceRestore.STDOUT_CAPTURE,
                'stderr': TierFenceRestore.STDERR_CAPTURE,
                'log_dir': TierFenceRestore.LOG_DIR,
                'log4j_config': TierFenceRestore.LOG4J_CONFIG,
                'config_file': TierFenceRestore.CONFIG_FILE,
                'kafka_opts': self.security_config.kafka_opts,
                'working_dir': TierFenceRestore.WORK_DIR}

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=TierFenceRestore.LOG_FILE)
        node.account.create_file(TierFenceRestore.LOG4J_CONFIG, log_config)

        # 1. Write file containing list of partitions to be fenced.
        first_partition = list(self.partition_ids_to_be_fenced)[0]
        topic_id_b64 = base64.urlsafe_b64encode(uuid.UUID(self.topic_id(first_partition)).bytes)
        fencing_input_file = "{}/trigger_fencing.input".format(TierFenceRestore.PERSISTENT_ROOT)
        input_tpids = []
        for partition in self.partition_ids_to_be_fenced:
            input_tpids.append('{},{},{}'.format(topic_id_b64, self.topic, partition))

        cmd = "echo -n '{}' > {}".format("\n".join(input_tpids), fencing_input_file)
        self.logger.debug(cmd)

        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line
        self.logger.debug(output)

        validate_offsets = False
        tier_config = self.kafka.nodes[0].config
        if tier_config[config_property.CONFLUENT_TIER_BACKEND] == "S3":
            validate_offsets = True

        # 2. write properties config file
        properties_file = "{}/trigger_fencing.properties".format(TierFenceRestore.PERSISTENT_ROOT)
        properties = []
        properties.append("bootstrap.servers={}".format(self.kafka.bootstrap_servers(self.kafka.security_protocol)))
        properties.append("confluent.tier.recovery.working.dir={}".format(TierFenceRestore.WORK_DIR))
        properties.append("confluent.tier.recovery.validate={}".format(validate_offsets))
        properties.append("confluent.tier.recovery.materialize=true")
        properties.append("confluent.tier.recovery.dump.events=false")

        broker_workdirs = []
        for knode in self.kafka.nodes:
            broker_workdirs.append("{}/snapshots/{}".format(TierFenceRestore.WORK_DIR, knode.account.hostname))
        properties.append("confluent.tier.recovery.broker.workdir.list={}".format(",".join(broker_workdirs)))

        # copy necessary properties from brokers
        copy_properties = [config_property.CONFLUENT_TIER_BACKEND, config_property.CONFLUENT_TIER_S3_REGION,
                config_property.CONFLUENT_TIER_S3_BUCKET, config_property.CONFLUENT_TIER_S3_PREFIX,
                config_property.CONFLUENT_TIER_GCS_BUCKET, config_property.CONFLUENT_TIER_GCS_REGION,
                config_property.CONFLUENT_TIER_GCS_PREFIX, config_property.CONFLUENT_TIER_GCS_CRED_FILE_PATH]
        for prop in copy_properties:
            if prop in tier_config:
                properties.append("{}={}".format(prop, tier_config[prop]))

        cmd = "echo -n '{}\n' > {}".format("\n".join(properties), properties_file)
        self.logger.debug(cmd)

        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line
        self.logger.debug(output)

        base_cmd = "export LOG_DIR=%(log_dir)s; " \
              "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j_config)s\"; " \
              "export KAFKA_OPTS=%(kafka_opts)s; " % args

        # 3. fence partitions that we wish to fence
        fence_cmd = base_cmd
        fence_cmd += self.path.script("kafka-run-class.sh kafka.tier.tools.TierPartitionStateFencingTrigger")
        fence_cmd += " --tier.config %s " % properties_file
        fence_cmd += " --file-fence-target-partitions %s" % fencing_input_file
        fence_cmd += " --output.json %s" % self.FENCE_OUTPUT
        fence_cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % args

        output = ""
        for line in node.account.ssh_capture(fence_cmd):
            output += line
        self.logger.debug(output)

        if not should_restore:
            return

        expected_fenced=len(self.partition_ids_to_be_fenced)
        wait_until(lambda: self.check_fenced_partitions(expected_fenced),
                   timeout_sec=120, backoff_sec=2,
                   err_msg="num fenced partitions was not reported as %s" % expected_fenced)

        # 4. copy states from brokers
        for kafka_node in self.kafka.nodes:
            # Create working directory
            node.account.ssh("mkdir -p %s" % TierFenceRestore.WORK_DIR)
            node.account.ssh("mkdir -p %s" % TierFenceRestore.SNAPSHOT_DIR)

            # Search and load all the states file ending with *.tierstate
            files = kafka_node.account.ssh_capture("find %s -name '*.%s'" % (self.metadata_states_dir,
                                                   TierFenceRestore.TIER_STATE_FILE_SUFFIX))
            self.logger.info("Found following tier-state files %s on %s", files, kafka_node.account.hostname)

            for state_file in files:
                state_file = state_file.strip('\n')
                topic_name = os.path.dirname(state_file)
                host_name = kafka_node.account.hostname
                file_name = os.path.basename(state_file)
                dest_dir = os.path.join(TierFenceRestore.SNAPSHOT_DIR, host_name, topic_name.split("/")[-1].strip())
                dst_file = os.path.join(dest_dir, file_name)
                self.logger.info("Copying %s:%s to %s:%s", kafka_node.account.hostname, state_file, node.account.hostname,
                                 dest_dir)
                node.account.ssh("mkdir -p %s" % dest_dir)
                kafka_node.account.copy_between(state_file, dst_file, node)

                if not node.account.isfile(dst_file):
                    self.logger.error("Failed to copy %s from %s to %s", dst_file, kafka_node.account.hostname, node.account.hostname)
                else:
                    self.logger.info("Copied state file %s", dst_file)

        # 5. compare states copied from brokers
        # It is assumed that the tool will choose the state with the highest end offset for each partition
        compare_cmd = base_cmd
        compare_cmd += self.path.script("kafka-run-class.sh kafka.tier.tools.TierMetadataComparator")
        compare_cmd += " --tier.config %s " % properties_file
        compare_cmd += " --input.json %s" % self.FENCE_OUTPUT
        compare_cmd += " --output.json %s" % self.COMPARATOR_OUTPUT
        compare_cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % args

        compare_output = ""
        for line in node.account.ssh_capture(compare_cmd):
            compare_output += line
        self.logger.debug(compare_output)

        # 6. restore states with the highest end offset that pass validation
        restore_cmd = base_cmd
        restore_cmd += self.path.script("kafka-run-class.sh kafka.tier.tools.TierPartitionStateRestoreTrigger")
        restore_cmd += " --tier.config %s " % properties_file
        restore_cmd += " --input.json %s" % self.COMPARATOR_OUTPUT
        restore_cmd += " --output.json %s" % self.RESTORE_OUTPUT
        restore_cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % args

        output = ""
        for line in node.account.ssh_capture(restore_cmd):
            output += line
        self.logger.debug(output)

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def _worker(self, idx, node):
        self.security_config = self.kafka.security_config.client_config(node=node,
                                                                        jaas_override_variables=self.jaas_override_variables)
        self.security_config.setup_node(node)
        node.account.ssh("mkdir -p %s" % TierFenceRestore.PERSISTENT_ROOT)
        self.fence_and_restore(node, self.should_restore)
        return 0

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
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf %s" % TierFenceRestore.PERSISTENT_ROOT)
        self.security_config.clean_node(node)

    def java_class_name(self):
        return "Tier"
