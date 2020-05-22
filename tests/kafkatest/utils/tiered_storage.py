from kafkatest.services.kafka import config_property

import os
from ducktape.utils.util import wait_until

import sys

S3_BACKEND = "S3"
GCS_BACKEND = "GCS"

TIER_DATA_LOG_DIR = "/mnt/kafka/kafka-data-logs-1"

def tier_server_props(backend, feature=True, enable=False,
                      metadata_replication_factor=3, metadata_num_partitions=50, hotset_bytes=1, hotset_ms=1,
                      log_segment_bytes=1024000, log_retention_check_interval=5000, log_roll_time=3000,
                      prefer_tier_fetch_ms=-1, hotset_roll_min_bytes=None,
                      tier_delete_check_interval=1000, tier_bucket_prefix=None):
    """Helper for building server_prop_overrides in Kafka tests that enable tiering"""
    props = [
        # tiered storage does not support multiple logdirs
        [config_property.LOG_DIRS, TIER_DATA_LOG_DIR],
        [config_property.LOG_SEGMENT_BYTES, log_segment_bytes],
        [config_property.LOG_ROLL_TIME_MS, log_roll_time],
        [config_property.LOG_RETENTION_CHECK_INTERVAL_MS, log_retention_check_interval]]


    # avoid setting backend if feature is not enabled, as a given backend may
    # be an invalid option for a version we are upgrading from
    if feature:
        props += [[config_property.CONFLUENT_TIER_TOPIC_DELETE_CHECK_INTERVAL_MS, tier_delete_check_interval]]

        if hotset_roll_min_bytes:
            props += [[config_property.CONFLUENT_TIER_SEGMENT_ROLL_MIN_BYTES, hotset_roll_min_bytes]]

        props += [[config_property.CONFLUENT_TIER_FEATURE, feature],
                  [config_property.CONFLUENT_TIER_ENABLE, enable],
                  [config_property.CONFLUENT_TIER_LOCAL_HOTSET_BYTES, hotset_bytes],
                  [config_property.CONFLUENT_PREFER_TIER_FETCH_MS, prefer_tier_fetch_ms],
                  [config_property.CONFLUENT_TIER_LOCAL_HOTSET_MS, hotset_ms],
                  [config_property.CONFLUENT_TIER_METADATA_REPLICATION_FACTOR, metadata_replication_factor],
                  [config_property.CONFLUENT_TIER_METADATA_NUM_PARTITIONS, metadata_num_partitions]]

        if backend == S3_BACKEND:
            props += [[config_property.CONFLUENT_TIER_BACKEND, S3_BACKEND],
                      [config_property.CONFLUENT_TIER_S3_BUCKET, "confluent-tier-system-test"],
                      [config_property.CONFLUENT_TIER_S3_REGION, "us-west-2"]]
            if tier_bucket_prefix:
                props += [[config_property.CONFLUENT_TIER_S3_PREFIX, tier_bucket_prefix]]

        elif backend == GCS_BACKEND:
            props += [[config_property.CONFLUENT_TIER_BACKEND, GCS_BACKEND],
                      [config_property.CONFLUENT_TIER_GCS_BUCKET, "confluent-tier-system-test-us-west1"],
                      [config_property.CONFLUENT_TIER_GCS_REGION, "us-west1"],
                      [config_property.CONFLUENT_TIER_GCS_CRED_FILE_PATH, "/vagrant/gcs_credentials.json"]]
            if tier_bucket_prefix:
                props += [[config_property.CONFLUENT_TIER_GCS_PREFIX, tier_bucket_prefix]]

    return props

def tier_set_configs(kafka, backend, **server_props_kwargs):
    """Helper for setting tier related configs directly on kafka service. Useful if we want to modify them directly later"""
    configs = tier_server_props(backend, **server_props_kwargs)
    for node in kafka.nodes:
        for config in configs:
            node.config[config[0]] = config[1]

class TieredStorageMetric:
    def __init__(self, mbean, attribute):
        self.mbean = mbean
        self.attribute = attribute

    def __str__(self):
        return self.mbean + ":" + self.attribute

class TieredStorageMetricsRegistry:
    ARCHIVER_LAG = TieredStorageMetric("kafka.tier.tasks.archive:type=TierArchiver,name=TotalLag", "Value")
    ARCHIVER_LAG_WITHOUT_ERROR_PARTITIONS = TieredStorageMetric("kafka.tier.tasks.archive:type=TierArchiver,name=TotalLagWithoutErrorPartitions", "Value")
    FETCHER_BYTES_FETCHED = TieredStorageMetric("kafka.server:type=TierFetcher", "BytesFetchedTotal")
    ARCHIVER_PARTITIONS_IN_ERROR = TieredStorageMetric("kafka.tier.tasks:type=TierTasks,name=NumPartitionsInError", "Value")
    TIER_TASKS_HEARTBEAT = TieredStorageMetric("kafka.tier.tasks:type=TierTasks,name=HeartbeatMs", "Value")
    TIER_TOPIC_MANAGER_HEARTBEAT = TieredStorageMetric("kafka.server:type=TierTopicConsumer", "HeartbeatMs")
    TIER_TOPIC_MANAGER_NUM_FENCED_PARTITIONS = TieredStorageMetric("kafka.server:type=TierTopicConsumer", "ErrorPartitions")
    DELETED_PARTITIONS_COORDINATOR_HEARTBEAT = TieredStorageMetric("kafka.tier:type=TierDeletedPartitionsCoordinator,name=HeartbeatMs", "Value")
    DELETED_PARTITIONS_COORDINATOR_INPROGRESS_DELETIONS = TieredStorageMetric("kafka.tier:type=TierDeletedPartitionsCoordinator,name=TierNumInProgressPartitionDeletions", "Value")
    DELETED_PARTITIONS_COORDINATOR_QUEUED_DELETIONS = TieredStorageMetric("kafka.tier:type=TierDeletedPartitionsCoordinator,name=TierNumQueuedPartitionDeletions", "Value")

    ALL_MBEANS = [ARCHIVER_LAG.mbean,
            ARCHIVER_LAG_WITHOUT_ERROR_PARTITIONS.mbean,
            FETCHER_BYTES_FETCHED.mbean,
            ARCHIVER_PARTITIONS_IN_ERROR.mbean,
            TIER_TOPIC_MANAGER_HEARTBEAT.mbean,
            TIER_TOPIC_MANAGER_NUM_FENCED_PARTITIONS.mbean,
            TIER_TASKS_HEARTBEAT.mbean,
            DELETED_PARTITIONS_COORDINATOR_HEARTBEAT.mbean,
            DELETED_PARTITIONS_COORDINATOR_INPROGRESS_DELETIONS.mbean,
            DELETED_PARTITIONS_COORDINATOR_QUEUED_DELETIONS.mbean]

    ALL_ATTRIBUTES = [ARCHIVER_LAG.attribute,
            ARCHIVER_LAG_WITHOUT_ERROR_PARTITIONS.attribute,
            FETCHER_BYTES_FETCHED.attribute,
            ARCHIVER_PARTITIONS_IN_ERROR.attribute,
            TIER_TOPIC_MANAGER_HEARTBEAT.attribute,
            TIER_TOPIC_MANAGER_NUM_FENCED_PARTITIONS.attribute,
            TIER_TASKS_HEARTBEAT.attribute,
            DELETED_PARTITIONS_COORDINATOR_HEARTBEAT.attribute,
            DELETED_PARTITIONS_COORDINATOR_INPROGRESS_DELETIONS.attribute,
            DELETED_PARTITIONS_COORDINATOR_QUEUED_DELETIONS.attribute]

    @staticmethod
    def log_tier_size(topic, partition):
        return TieredStorageMetric("kafka.log:type=Log,name=TierSize,topic={},partition={}".format(topic, partition), "Value")

    @staticmethod
    def log_local_size(topic, partition):
        return TieredStorageMetric("kafka.log:type=Log,name=Size,topic={},partition={}".format(topic, partition), "Value")

    @staticmethod
    def num_log_segments(topic, partition):
        return TieredStorageMetric("kafka.log:type=Log,name=NumLogSegments,topic={},partition={}".format(topic, partition), "Value")

class TierSupport():
    """Tiered storage helpers. Mix in only with KafkaService-based tests"""

    def configure_tiering(self, bucket, **server_props_kwargs):
        self.kafka.jmx_object_names = TieredStorageMetricsRegistry.ALL_MBEANS
        self.kafka.jmx_attributes = TieredStorageMetricsRegistry.ALL_ATTRIBUTES
        self.kafka.server_prop_overides = tier_server_props(bucket, **server_props_kwargs)

    def tiering_completed_prefer_fetch(self, topic, segment_size, partitions=[0], ignore_error_partitions=False):
        # check that tiering started first - tiered log size should be non-zero
        if not self.tiering_started(topic, partitions):
            return False

        self.kafka.read_jmx_output_all_nodes()
        for partition in partitions:
            log_local_size_metric = str(TieredStorageMetricsRegistry.log_local_size(topic, partition))
            log_tier_size_metric = str(TieredStorageMetricsRegistry.log_tier_size(topic, partition))
            for node in self.kafka.nodes:
                last_jmx_entry = self.kafka.last_jmx_item(self.kafka.idx(node))
                archiver_lag = None
                if ignore_error_partitions:
                    archiver_lag = last_jmx_entry.get(str(TieredStorageMetricsRegistry.ARCHIVER_LAG_WITHOUT_ERROR_PARTITIONS), -1)
                else:
                    archiver_lag = last_jmx_entry.get(str(TieredStorageMetricsRegistry.ARCHIVER_LAG), -1)
                local_size = last_jmx_entry.get(log_local_size_metric, -1)
                tier_size = last_jmx_entry.get(log_tier_size_metric, -1)
                # we do not delete locally for the prefer tier fetch runs, so the sizes
                # should match other than the active segment
                if archiver_lag != 0 or local_size - tier_size > segment_size:
                    self.logger.debug("Archiving not complete for partition " + str(partition))
                    return False
        return True

    def check_heartbeat(self, last_jmx_entry, metric, cutoff_ms):
        heartbeat = last_jmx_entry.get(str(metric), -1)
        if heartbeat == -1 or heartbeat > cutoff_ms:
            self.logger.debug(str(metric) + " greater than cutoff " + str(cutoff_ms))
            return False
        return True

    def check_cluster_state(self):
        self.kafka.read_jmx_output_all_nodes()
        for node_stats in self.kafka.jmx_stats:
            last_jmx_entry = sorted(node_stats.items(), key=lambda kv: kv[0])[-1][1]

            if not (self.check_heartbeat(last_jmx_entry, TieredStorageMetricsRegistry.TIER_TOPIC_MANAGER_HEARTBEAT, 2000)
                    and self.check_heartbeat(last_jmx_entry, TieredStorageMetricsRegistry.DELETED_PARTITIONS_COORDINATOR_HEARTBEAT, 90000)):
                return False

            archiver_partitions_in_error = last_jmx_entry.get(str(TieredStorageMetricsRegistry.ARCHIVER_PARTITIONS_IN_ERROR), -1)
            if archiver_partitions_in_error != 0:
                self.logger.debug("Archiver " + str(archiver_partitions_in_error) + " partitions in error")
                return False

        if not self.check_fenced_partitions(0):
            return False

        return True

    def check_fenced_partitions(self, expected_val):
        self.kafka.read_jmx_output_all_nodes()
        metric = str(TieredStorageMetricsRegistry.TIER_TOPIC_MANAGER_NUM_FENCED_PARTITIONS)
        for node_stats in self.kafka.jmx_stats:
            last_jmx_entry = sorted(node_stats.items(), key=lambda kv: kv[0])[-1][1]
            num_fenced_partitions = last_jmx_entry.get(metric, None)
            if num_fenced_partitions != expected_val:
                self.logger.debug(
                    "Found " + str(num_fenced_partitions) + " for metric: " + metric +
                    ", but expected: " + str(expected_val))
                return False
        return True

    def tiering_completed(self, topic, partitions=[0], ignore_error_partitions=False):
	"""Ensure that:
	   1. Archive lag is 0 on all brokers
	   2. One log segment is present on one broker (the leader)
	   3. Fewer than two log segments are present on other brokers.
	      The segments between replicas will not necessarily align."""

        # check that tiering started first - tiered log size should be non-zero
        if not self.tiering_started(topic, partitions):
            return False

        self.kafka.read_jmx_output_all_nodes()
        for partition in partitions:
            num_log_segments_metric = str(TieredStorageMetricsRegistry.num_log_segments(topic, partition))
            leader_idx = self.kafka.idx(self.kafka.leader(topic, partition))
            if leader_idx is None:
                return False

            leader_num_log_segments = self.kafka.last_jmx_item(leader_idx).get(num_log_segments_metric, -1)
            if leader_num_log_segments != 1:
                self.logger.debug("Archiving not complete as leader has "
                        + str(leader_num_log_segments) + " local log segments for partition " + str(partition))
                return False

            for node in self.kafka.nodes:
                last_jmx_entry = self.kafka.last_jmx_item(self.kafka.idx(node))
                archiver_lag = None
                if ignore_error_partitions:
                    archiver_lag = last_jmx_entry.get(
                        str(TieredStorageMetricsRegistry.ARCHIVER_LAG_WITHOUT_ERROR_PARTITIONS), -1)
                else:
                    archiver_lag = last_jmx_entry.get(str(TieredStorageMetricsRegistry.ARCHIVER_LAG), -1)
                num_log_segments = last_jmx_entry.get(num_log_segments_metric, -1)
                if archiver_lag != 0 or num_log_segments > 2:
                    self.logger.debug("Archiving not complete for partition " + str(partition) + ". lag: "
                            + str(archiver_lag) + " num_log_segments: " + str(num_log_segments))
                    return False

        return True

    def tiering_started(self, topic, partitions=[0]):
        self.kafka.read_jmx_output_all_nodes()
        for partition in partitions:
            tier_size = self.kafka.maximum_jmx_value.get(str(TieredStorageMetricsRegistry.log_tier_size(topic, partition)), -1)
            if tier_size <= 0:
                return False

        return True

    def tiering_triggered_local_deletion(self, topic, partitions=[0]):
        log_segment_to_be_searched = "00000000000000000000.log"
        # tiered storage does not support multiple logdirs
        data_dir_list = [os.path.join(TIER_DATA_LOG_DIR, "%s-%d" % (topic, partition)) for partition in partitions]
        for knode in self.kafka.nodes:
            for logdir in data_dir_list:
                output = knode.account.ssh_capture("find %s -type f -name '%s' | wc -l" %
                                                   (logdir, log_segment_to_be_searched), callback=int)
                for length in output:
                    if length > 0:
                        self.logger.debug("Deletion not started for directory: %s" % logdir)
                        return False
        return True

    def add_log_metrics(self, topic, partitions=[0]):
        """Log-specific beans are not available at server startup"""
        names = self.kafka.jmx_object_names
        attrs = self.kafka.jmx_attributes
        names = [] if names == None else names
        attrs.append("Value") if "Value" not in attrs else attrs
        for p in partitions:
            names += [TieredStorageMetricsRegistry.log_local_size(topic, p).mbean,
                      TieredStorageMetricsRegistry.num_log_segments(topic, p).mbean,
                      TieredStorageMetricsRegistry.log_tier_size(topic, p).mbean]

    def add_error_metrics(self):
        self.kafka.jmx_object_names += [TieredStorageMetricsRegistry.TIER_TOPIC_MANAGER_NUM_FENCED_PARTITIONS.mbean,
                TieredStorageMetricsRegistry.ARCHIVER_PARTITIONS_IN_ERROR.mbean,
                TieredStorageMetricsRegistry.TIER_TOPIC_MANAGER_HEARTBEAT.mbean,
                TieredStorageMetricsRegistry.DELETED_PARTITIONS_COORDINATOR_HEARTBEAT.mbean]
        self.kafka.jmx_attributes += [TieredStorageMetricsRegistry.TIER_TOPIC_MANAGER_NUM_FENCED_PARTITIONS.attribute,
                TieredStorageMetricsRegistry.ARCHIVER_PARTITIONS_IN_ERROR.attribute,
                TieredStorageMetricsRegistry.TIER_TOPIC_MANAGER_HEARTBEAT.attribute,
                TieredStorageMetricsRegistry.DELETED_PARTITIONS_COORDINATOR_HEARTBEAT.attribute]

    def remove_log_metrics(self, topic, partitions=[0]):
        """Removes log related metrics for given topic partitions.
           This is necessary when these topics are deleted as jmx tool will return
           all metrics or nothing
        """
        remove = set([])
        for p in partitions:
            remove.add(TieredStorageMetricsRegistry.log_local_size(topic, p).mbean)
            remove.add(TieredStorageMetricsRegistry.num_log_segments(topic, p).mbean)
            remove.add(TieredStorageMetricsRegistry.log_tier_size(topic, p).mbean)
        self.kafka.jmx_object_names = [item for item in self.kafka.jmx_object_names if item not in remove]

    def restart_jmx_tool(self):
        for node in self.kafka.nodes:
            node.account.kill_java_processes(self.kafka.jmx_class_name(), clean_shutdown=False, allow_fail=True)
            idx = self.kafka.idx(node)
            self.kafka.started[idx-1] = False
            self.kafka.start_jmx_tool(idx, node)

    def list_s3_contents(self):
        node = self.kafka.nodes[0]
        bucket = node.config[config_property.CONFLUENT_TIER_S3_BUCKET]
        prefix = node.config[config_property.CONFLUENT_TIER_S3_PREFIX]
        cmd = "aws s3 ls --recursive {}/{}".format(bucket, prefix)
        for line in node.account.ssh_capture(cmd, allow_fail=True):
            yield line.rstrip()

    def setup_gsutil(self):
        for node in self.kafka.nodes:
            cmd = "gcloud auth activate-service-account --key-file %s" % node.config[config_property.CONFLUENT_TIER_GCS_CRED_FILE_PATH]
            node.account.ssh_capture(cmd, allow_fail=False)

    def list_gcs_contents(self):
        node = self.kafka.nodes[0]
        bucket = node.config[config_property.CONFLUENT_TIER_GCS_BUCKET]
        prefix = node.config[config_property.CONFLUENT_TIER_GCS_PREFIX]
        cmd = "gsutil ls -r gs://{}/{}".format(bucket, prefix)
        for line in node.account.ssh_capture(cmd, allow_fail=True):
            yield line.rstrip()

    def deletions_in_progress(self):
        self.kafka.read_jmx_output_all_nodes()
        for node in self.kafka.nodes:
            last_jmx_entry = self.kafka.last_jmx_item(self.kafka.idx(node))
            queued_deletions = last_jmx_entry.get(
                str(TieredStorageMetricsRegistry.DELETED_PARTITIONS_COORDINATOR_INPROGRESS_DELETIONS), sys.maxint)
            if queued_deletions > 0:
                return True

            in_progress_deletions = last_jmx_entry.get(
                str(TieredStorageMetricsRegistry.DELETED_PARTITIONS_COORDINATOR_QUEUED_DELETIONS), sys.maxint)
            if in_progress_deletions > 0:
                return True

        return False

    def object_deletions_completed(self, backend, timeout_sec=1800, backoff_sec=2):
        if backend == S3_BACKEND:
            # we set the timeout to be very large here to ensure S3's ListBucket consistency properties
            # have suffient time to show object deletion
            wait_until(lambda: len(list(self.list_s3_contents())) == 0,
                       timeout_sec=timeout_sec, backoff_sec=backoff_sec, err_msg="deletion has not completed yet " +
                       str(list(self.list_s3_contents())))
        elif backend == GCS_BACKEND:
            self.setup_gsutil()
            wait_until(lambda: list(self.list_gcs_contents()) == ["CommandException: One or more URLs matched no objects."],
            timeout_sec=timeout_sec, backoff_sec=backoff_sec,
            err_msg="deletion has not completed yet " + str(list(self.list_gcs_contents())))

        self.restart_jmx_tool()
        wait_until(lambda: self.deletions_in_progress() == False,
                   timeout_sec=720, backoff_sec=2, err_msg="deletions still in progress according to jmx metrics")
