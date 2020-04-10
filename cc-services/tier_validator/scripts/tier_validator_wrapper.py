# Copyright 2020 Confluent Inc.

import subprocess
import os
import uuid
import datetime
import logging
import argparse
import sys


class TierSoakUtils(object):
    """
    This class serves as a utility placeholder by containing all the common stateless infrastructure methods
    """
    logger = logging.getLogger(__name__)

    def exec_cmd(self, cmd, result_needed=True, dry_run=False, shell=False, validator_run_log_file=None,
                 log_file_mode="w"):
        cmd_with_params = "{cmd=%s, dry_run=%s, shell=%s, log_file=%s, log_open_mode=%s}" \
                          % (cmd, str(dry_run), str(shell), validator_run_log_file, log_file_mode)
        try:
            self.logger.debug("Executing cmd_with_params: %s" % cmd_with_params)
            cmd_list = cmd.split() if not shell else cmd
            if dry_run:
                self.logger.debug("Not executing %s because of dry-run" % cmd)
                return True
            elif result_needed:
                output = subprocess.check_output(cmd_list, shell=shell)
                return [line.rstrip() for line in output.split(os.linesep) if len(line)]  # Stripping away empty lines
            elif validator_run_log_file:
                with open(validator_run_log_file, log_file_mode) as log_file_handle:
                    return_code = subprocess.call(cmd_list, shell=shell, stdout=log_file_handle, stderr=log_file_handle)
                    return return_code == 0
            else:
                subprocess.check_call(cmd_list, shell=shell)
                return True
        except subprocess.CalledProcessError as e:
            self.logger.error("Received exception during cmd_with_params: %s execution: %s" % (cmd_with_params, str(e)))
            return False


class TierMetadataSoakConfig(object):
    """
    This class consists of the various configuration objects necessary for the Validator script to succeed.
    This includes various file paths and the run configurations.
    Some of these properties are currently configurable via the cmdline and more can be added depending on necessity.
    """
    # The following can be set via the cmdline options (refer to the `parse_arguments` method)
    KAFKA_RUN_CLASS_PATH = "/opt/confluent/bin/kafka-run-class.sh"
    TIER_METADATA_VALIDATOR_CLS_NAME = "kafka.tier.tools.TierMetadataValidator"
    KAFKA_BOOTSTRAP_SERVER_PORT = 9071
    KAFKA_BOOTSTRAP_SERVER_SUFFIX = "kafka"
    KUBECTL_NAMESPACE = ""
    BROKER_LIST = []
    TIER_TOPIC_PARTITION_LIST = []
    # The tier validator would be run from ${LOCAL_SEQUENTIAL_WORKING_DIR}
    # In case the cmdline option is not provided, this script will generate the workdir in the following pattern:
    # /mnt/tier_validator/workdir/yyyy-mm-dd-xxx/ where <xxx> will be zero-padded sequence numbers
    LOCAL_SEQUENTIAL_WORKING_DIR = ""
    VERIFY_TIER_STORAGE = False
    VERIFY_OFFSET = False

    # The following properties are currently configured within this script itself
    # We will pull all the tierstate files from the partitions within the following data directory
    KAFKA_REMOTE_DATA_DIR = "/mnt/data/data0/logs/"
    KAFKA_TIER_TOPIC_PARTITION_COUNT = 50
    _LOCAL_WORKING_DIR_PREFIX = "/mnt/tier_validator/workdir"
    # The snapshots dirs for each node would be at /mnt/tier_validator/workdir/kafka-{x}/snapshots
    SNAPSHOTS_DIR_SUFFIX = "snapshots"
    # The scratch dir will be used to store and expand the tar files received from remote brokers
    SCRATCH_DIR_SUFFIX = "scratch"
    # The runtime log of the validator tool for each kafka node would be at :
    # /mnt/tier_validator/workdir/kafka-{x}/${VALIDATOR_LOG_FILE}
    VALIDATOR_LOG_FILE = "run_validator.log"
    # "tierstate" is used as both prefix and suffix in various file names, hence, it is defined as a const here
    TIERSTATE_CONST_STR = "tierstate"
    REMOTE_TAR_FILE_LOC = "/tmp"
    # S3 specific arguments
    TIER_S3_BUCKET = ""
    TIER_S3_REGION = "us-west-2"
    # Session specific uuid
    SESSION_UUID = str(uuid.uuid4())
    # enabling module level logging
    logger = logging.getLogger(__name__)

    @property
    def KUBECTL_CMD_PREFIX(self):
        # This is a derived property, since the namespace can be configured via the cmdline
        return "kubectl -n %s" % self.KUBECTL_NAMESPACE

    def __init__(self, utils):
        self.utils = utils
        self.init_time = datetime.datetime.utcnow()
        if not self._maybe_generate_local_workdir():
            raise ValueError("Couldn't complete tool initialization")

    def _maybe_generate_local_workdir(self):
        if self.LOCAL_SEQUENTIAL_WORKING_DIR:
            # In case we pass the value through cmdline opts, we will try to make sure that the dir exists
            mkdir_cmd = "mkdir -p %s" % self.LOCAL_SEQUENTIAL_WORKING_DIR
            return self.utils.exec_cmd(mkdir_cmd, result_needed=False)
        else:
            current_utc_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
            zero_padding_count = 3
            existing_dir_cmd = r"find %s -type d -name '%s-*'" % (self._LOCAL_WORKING_DIR_PREFIX, current_utc_date)
            existing_dir_list = self.utils.exec_cmd(existing_dir_cmd, shell=True)
            sequential_dir_name = str(len(existing_dir_list) + 1).zfill(zero_padding_count) \
                if existing_dir_list else str(1).zfill(zero_padding_count)

            new_working_dir = os.path.join(self._LOCAL_WORKING_DIR_PREFIX,
                                           "%s-%s" % (current_utc_date, sequential_dir_name))
            if self.utils.exec_cmd("mkdir -p %s" % new_working_dir, result_needed=False):
                self.LOCAL_SEQUENTIAL_WORKING_DIR = new_working_dir
                self.logger.info("Initialised dir: %s for this run" % new_working_dir)
                return True
            else:
                self.logger.error("Failed to initialise dir: %s Quitting this run" % new_working_dir)
                return False


class TierMetadataSoakValidator(object):
    """
    This class contains the crux of the tooling for the validation.
    There are the following main aspects to this tool's functioning:
    1. Pre-run validation: Sanity checks, performing `get pods` operation to know the state of the cluster
    2. Validation: This is a 2-step process
        a. Establishes the folder structure per broker for the working and the snapshot directories
        b. Runs the TierMetadataValidator tool from the specified bin directory
    3. Post-run Validation: Verifies the validation from the exit-code of the validator run
    """
    # enabling module level logging
    logger = logging.getLogger(__name__)

    def __init__(self, config, utils):
        self.config = config
        self.utils = utils
        self.stats = dict()
        self.stats["remote_tar_files"] = dict()

    def _get_pods(self):
        cmd = "%s get pods -l type=kafka" % self.config.KUBECTL_CMD_PREFIX
        details = self.utils.exec_cmd(cmd)
        all_kafka_pods = [line.split()[0] for line in details[1:] if line.split()[2] == "Running"]
        return all_kafka_pods

    def _verify_dir(self, dir_name):
        ls_cmd = "ls -l %s" % dir_name
        return self.utils.exec_cmd(ls_cmd)

    def _pre_run_validation(self):
        # Whether kafka-run-class.sh is present
        if not self._verify_dir(self.config.KAFKA_RUN_CLASS_PATH):
            self.logger.error("kafka-run-class.sh not found!")
            return False
        # Verify there are live Kafka brokers within the pod
        kafka_pod_names = self._get_pods()
        if not kafka_pod_names:
            self.logger.error("No running kafka pods found in the cluster!")
            return False
        # Create working dirs for each pod
        all_snapshot_paths = [
            os.path.join(self.config.LOCAL_SEQUENTIAL_WORKING_DIR, pod_name, self.config.SNAPSHOTS_DIR_SUFFIX)
            for pod_name in kafka_pod_names]

        all_scratch_paths = [
            os.path.join(self.config.LOCAL_SEQUENTIAL_WORKING_DIR, pod_name, self.config.SCRATCH_DIR_SUFFIX)
            for pod_name in kafka_pod_names]

        mkdir_cmd = "mkdir -p -- %s" % ' '.join(all_snapshot_paths + all_scratch_paths)
        if not self.utils.exec_cmd(mkdir_cmd, result_needed=False):
            self.logger.error("Couldn't create all the working dirs!")
            return False
        return True

    def _generate_tar_file_name(self, pod_name):
        return "%s_%s_%s.tar" % (self.config.TIERSTATE_CONST_STR, self.config.SESSION_UUID, pod_name)

    def _prepare_tierstate_files(self, pod_name):
        self.logger.info("Beginning validation for broker: %s" % pod_name)
        kubectl_exec_cmd = "%s exec %s -- " % (self.config.KUBECTL_CMD_PREFIX, pod_name)
        tar_file_name = self._generate_tar_file_name(pod_name)
        remote_tar_file_path = os.path.join(self.config.REMOTE_TAR_FILE_LOC, tar_file_name)
        self.stats["remote_tar_files"][pod_name] = tar_file_name
        remote_tar_cmd = "tar -rvf %s" % remote_tar_file_path
        local_working_dir = os.path.join(self.config.LOCAL_SEQUENTIAL_WORKING_DIR, pod_name)
        # sample generated remote_find_with_tar_cmd:
        # "kubectl -n pkc-lgmzn exec kafka-0 -- find /mnt/data/data0/logs/ -type f -name '*.tierstate'
        # -exec tar -rvf /tmp/tierstate_5d6a4020-ff74-45f2-8c61-417aa6b5a33e_kafka-0.tar {} \;"
        remote_find_with_tar_cmd = r"%s find %s -type f -name '*.%s' -exec %s {} \;" % \
                                   (kubectl_exec_cmd, self.config.KAFKA_REMOTE_DATA_DIR,
                                    self.config.TIERSTATE_CONST_STR, remote_tar_cmd)
        if self.utils.exec_cmd(remote_find_with_tar_cmd, shell=True, result_needed=False):
            self.logger.debug("Successfully generated tar file: %s on %s" % (remote_tar_file_path, pod_name))
        else:
            self.logger.error("Remote tar file generation failed on broker: %s, skipping validation." % pod_name)
            return False

        # Once the tar file is generated on the broker, we will copy it to the local tier-validator pod
        # The remote tar file will be copied to ${LOCAL_SEQUENTIAL_WORKING_DIR}/kafka-{x}/scratch
        # The directory structure after untar needs to be:
        # ${LOCAL_SEQUENTIAL_WORKING_DIR}/kafka-{x}/snapshots/{topic_name}/*.tierstate
        local_tar_file_dir = os.path.join(local_working_dir, self.config.SCRATCH_DIR_SUFFIX)
        local_tar_file_path = os.path.join(local_tar_file_dir, tar_file_name)
        kubectl_cp_cmd = "%s cp %s:%s %s" % (self.config.KUBECTL_CMD_PREFIX, pod_name, remote_tar_file_path,
                                             local_tar_file_path)
        if self.utils.exec_cmd(kubectl_cp_cmd, shell=True):
            self.logger.debug("Successfully copied tar file: %s at %s" % (tar_file_name, local_tar_file_dir))
        else:
            self.logger.error("Error copying remote tar file from: %s on broker: %s" % (remote_tar_file_path, pod_name))
            return False
        local_untar_cmd = "tar -C %s -xvf %s" % (local_tar_file_dir, local_tar_file_path)
        if self.utils.exec_cmd(local_untar_cmd):
            self.logger.debug("Successfully extracted tar file: %s at %s" % (tar_file_name, local_tar_file_dir))
        else:
            self.logger.error("Error extracting tar file: %s at: %s" % (tar_file_name, local_tar_file_dir))
            return False
        local_snapshot_dir = os.path.join(local_working_dir, self.config.SNAPSHOTS_DIR_SUFFIX)
        local_mv_cmd = r"mv %s/%s/* %s" % (local_tar_file_dir, self.config.KAFKA_REMOTE_DATA_DIR, local_snapshot_dir)
        if self.utils.exec_cmd(local_mv_cmd, result_needed=False, shell=True):
            self.logger.debug("Successfully moved tierstate files to %s" % local_snapshot_dir)
        else:
            self.logger.error("Error moving extracted files to %s" % local_snapshot_dir)
            return False
        self.logger.info("Preparations complete for validation on broker: %s" % pod_name)
        return True

    def _run_tier_metadata_validator(self, pod_name, broker_id, tier_topic_partition_id=None):
        local_working_dir = os.path.join(self.config.LOCAL_SEQUENTIAL_WORKING_DIR, pod_name)
        bootstrap_server = "%s.%s:%d" % (pod_name,
                                         self.config.KAFKA_BOOTSTRAP_SERVER_SUFFIX,
                                         self.config.KAFKA_BOOTSTRAP_SERVER_PORT)
        validator_cmd = "%s %s --snapshot-states-files=false --bootstrap=server=%s --working-dir=%s " \
                        "--validate-tier-storage=%s --validate-tier-storage-offset=%s --broker.id=%d " \
                        "--confluent.tier.s3.bucket=%s --confluent.tier.s3.region=%s" % \
                        (self.config.KAFKA_RUN_CLASS_PATH,
                         self.config.TIER_METADATA_VALIDATOR_CLS_NAME,
                         bootstrap_server,
                         local_working_dir,
                         str(self.config.VERIFY_TIER_STORAGE).lower(),
                         str(self.config.VERIFY_OFFSET).lower(),
                         broker_id,
                         self.config.TIER_S3_BUCKET,
                         self.config.TIER_S3_REGION)

        if tier_topic_partition_id is not None:
            validator_cmd = "%s --tier-state-topic-partition=%s" % (validator_cmd, str(tier_topic_partition_id))

        validator_log_file = os.path.join(local_working_dir, self.config.VALIDATOR_LOG_FILE)
        logging_suffix = "Broker: %s, Logfile: %s" % (pod_name, validator_log_file)
        if tier_topic_partition_id is not None:
            logging_suffix = "%s and tier_topic_partition_id: %d" % (logging_suffix, tier_topic_partition_id)
        self.logger.debug("Beginning validator for: %s" % logging_suffix)
        log_file_mode = "a" if tier_topic_partition_id is not None else "w"
        if self.utils.exec_cmd(validator_cmd, result_needed=False, validator_run_log_file=validator_log_file,
                               dry_run=False, log_file_mode=log_file_mode):
            self.logger.debug("Completed validator for : %s" % logging_suffix)
            return True
        else:
            self.logger.error("Validator couldn't complete for: %s" % logging_suffix)
            return False

    def _verify_validator_results(self, brokers):
        # TODO: Implement deep validation logic. For now, we assume that if the run completed, then it's validated
        if "run_validator_results" in self.stats:
            results = self.stats["run_validator_results"]
            brokers_validated = [broker for broker in results if results[broker]]
            return brokers_validated, list(set(brokers) - set(brokers_validated))
        else:
            return [], brokers

    def run_post_validator(self, brokers):
        self.logger.info("Beginning post run phase")
        self.stats["brokers_ran"] = brokers
        self.stats["brokers_passed"], self.stats["brokers_failed"] = self._verify_validator_results(brokers)
        self.stats["start_time"] = self.config.init_time
        self.stats["end_time"] = datetime.datetime.utcnow()
        self.stats["total_time_taken"] = int((self.stats["end_time"] - self.stats["start_time"]).total_seconds())
        self._print_results()

    def _print_results(self):
        print_str = """%s
        ****************** Run Results ******************
        Validator binary used: %s
        Local Working Directory: %s
        Timestamp now: %s
        Total time taken: %dsec
        Brokers to be Validated: %s
        Brokers actually Validated: %s
        Tier Topic Partitions Validated: %s
        Brokers Validation Passed: %s
        Brokers Validation Failed: %s
        ****************** End of Script ******************
        """ % (os.linesep,
               self.config.KAFKA_RUN_CLASS_PATH,
               self.config.LOCAL_SEQUENTIAL_WORKING_DIR,
               self.stats["end_time"],
               self.stats["total_time_taken"],
               str(self.stats["brokers_to_be_validated"]),
               str(self.stats["brokers_ran"]),
               str(self.stats["partitions_to_be_validated"]),
               str(self.stats["brokers_passed"]),
               str(self.stats["brokers_failed"]))

        self.logger.info(print_str)

    def run_validator(self):
        # Verify kafka binaries are installed
        if self._pre_run_validation():
            kafka_broker_list = self._get_pods()
            self.stats["brokers_to_be_validated"] = self.config.BROKER_LIST if self.config.BROKER_LIST \
                else kafka_broker_list
            self.stats["partitions_to_be_validated"] = self.config.TIER_TOPIC_PARTITION_LIST \
                if self.config.TIER_TOPIC_PARTITION_LIST else range(self.config.KAFKA_TIER_TOPIC_PARTITION_COUNT)
            results = {broker: False for broker in self.stats["brokers_to_be_validated"]}
            for b_id, broker in enumerate(sorted(self.stats["brokers_to_be_validated"])):
                # File operations including copying the tierstate from the brokers
                if not self._prepare_tierstate_files(broker):
                    self.logger.error("Couldn't prepare tierstate files for broker: %s. Skipping validation." % broker)
                    continue
                # Run the validation tool for each broker pod
                partitions_successfully_validated = []
                for partition_id in self.stats["partitions_to_be_validated"]:
                    if self._run_tier_metadata_validator(broker, b_id + 1, tier_topic_partition_id=partition_id):
                        partitions_successfully_validated.append(partition_id)
                if set(partitions_successfully_validated) == set(self.stats["partitions_to_be_validated"]):
                    results[broker] = True
            self.stats["run_validator_results"] = results
            return results

    def clean_up(self):
        for broker in self.stats["remote_tar_files"]:
            remote_tar_file = self.stats["remote_tar_files"][broker]
            kubectl_delete_cmd = "%s exec %s -- find %s -type f -name %s -delete" % (self.config.KUBECTL_CMD_PREFIX,
                                                                                      broker,
                                                                                      self.config.REMOTE_TAR_FILE_LOC,
                                                                                      remote_tar_file)
            if self.utils.exec_cmd(kubectl_delete_cmd, shell=True, result_needed=False):
                self.logger.debug("Successfully deleted remote tar file: %s at %s" % (remote_tar_file, broker))
            else:
                self.logger.error("Error cleaning up file: %s on broker: %s" % (remote_tar_file, broker))


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_arguments():
    configurable_args = [
        {"name": "confluent.tier.s3.bucket", "mapped_property": "TIER_S3_BUCKET",
         "help": "S3 bucket for TierMetadataValidator for backend validation", "type": str, "required": True},
        {"name": "kubectl-namespace", "mapped_property": "KUBECTL_NAMESPACE",
         "help": "The namespace to run the kubectl commands", "type": str, "required": True},
        {"name": "kafka-run-class-path", "mapped_property": "KAFKA_RUN_CLASS_PATH",
         "help": "The absolute path of the kafka-run-class.sh, defaults to: '/opt/confluent/bin/kafka-run-class.sh'",
         "type": str},
        {"name": "tier-metadata-validator-cls-name", "mapped_property": "TIER_METADATA_VALIDATOR_CLS_NAME",
         "help": "The fully qualified classname of the validator Jar, defaults to "
                 "'kafka.tier.tools.TierMetadataValidator'",
         "type": str},
        {"name": "kafka-bootstrap-server-port", "mapped_property": "KAFKA_BOOTSTRAP_SERVER_PORT",
         "help": "The port number of the bootstrap server, defaults to 9071", "type": int},
        {"name": "kafka-bootstrap-server-suffix", "mapped_property": "KAFKA_BOOTSTRAP_SERVER_SUFFIX",
         "help": "The kubectl specific suffix for the address resolution of the specific pods, defaults to 'kafka'",
         "type": str},
        {"name": "kafka-remote-data-dir", "mapped_property": "KAFKA_REMOTE_DATA_DIR",
         "help": "The remote log directories on the kafka brokers, defaults to '/mnt/data/data0/logs/'",
         "type": str},
        {"name": "broker-list", "mapped_property": "BROKER_LIST",
         "help": "List of brokers to specifically run the validator against  (eg: kafka-0,kafka-1). "
                 "Defaults to all the brokers in the cluster",
         "type": str},
        {"name": "tier-topic-partition-list", "mapped_property": "TIER_TOPIC_PARTITION_LIST",
         "help": "List of tier topic partition ids to be validated (eg: 1,23,4,49). Defaults to 0 through 49",
         "type": int},
        {"name": "local-sequential-working-dir", "mapped_property": "LOCAL_SEQUENTIAL_WORKING_DIR",
         "help": "The local working dir from which the tool will run. "
                 "Defaults to /mnt/tier_validator/workdir/yyyy-mm-dd-xxx/ where <xxx> will be zero-padded seq numbers",
         "type": str},
        {"name": "confluent.tier.s3.region", "mapped_property": "TIER_S3_REGION",
         "help": "S3 region to pass to the TierMetadataValidator for backend validation, defaults to 'us-west-2'",
         "type": str},
        {"name": "validate-tier-storage", "mapped_property": "VERIFY_TIER_STORAGE",
         "help": "Flag to verify existence of object on cloud tier store, defaults to False", "type": bool},
        {"name": "validate-tier-storage-offset", "mapped_property": "VERIFY_OFFSET",
         "help": "Flag to verify offset ranges within the segment files, defaults to False", "type": bool}
    ]

    parser = argparse.ArgumentParser(description='Python wrapper utility to automate the run of TierMetadataValidator')
    for config in configurable_args:
        required = False if "required" not in config else config["required"]
        parser.add_argument("--%s" % config["name"], help=config["help"], dest=config["mapped_property"],
                            required=required)
    parsed_arg_dict = vars(parser.parse_args())
    for config in configurable_args:
        prop_key = config["mapped_property"]
        prop_value = parsed_arg_dict[prop_key]
        if prop_value and prop_key.endswith("LIST"):
            parsed_value_list = [(config["type"])(value) for value in prop_value.split(",")]
            setattr(TierMetadataSoakConfig, prop_key, parsed_value_list)
        elif prop_value:
            if config["type"] == bool:
                setattr(TierMetadataSoakConfig, prop_key, str2bool(prop_value))
            else:
                typed_value = (config["type"])(prop_value)
                setattr(TierMetadataSoakConfig, prop_key, typed_value)


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s')
    stdout_handler.setFormatter(formatter)
    root.addHandler(stdout_handler)


if __name__ == '__main__':
    setup_logging()
    parse_arguments()
    tier_utils = TierSoakUtils()
    validator_config = TierMetadataSoakConfig(tier_utils)
    validator = TierMetadataSoakValidator(validator_config, tier_utils)
    try:
        validation_result = validator.run_validator()
        if validation_result:
            validator.run_post_validator(list(validation_result.keys()))
    finally:
        validator.clean_up()
