from kafkatest.services.kafka import config_property

import sys

S3_BACKEND = "S3"
GCS_BACKEND = "GCS"

def tier_server_props(backend, feature=True, enable=False,
                      metadata_replication_factor=3, hotset_bytes=1, hotset_ms=1,
                      log_segment_bytes=1024000, log_retention_check_interval=5000, log_roll_time=3000,
                      hotset_roll_min_bytes=None, tier_delete_check_interval=1000, tier_bucket_prefix=None):
    """Helper for building server_prop_overrides in Kafka tests that enable tiering"""
    props = [
        # tiered storage does not support multiple logdirs
        [config_property.LOG_DIRS, "/mnt/kafka/kafka-data-logs-1"],
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
                  [config_property.CONFLUENT_TIER_LOCAL_HOTSET_MS, hotset_ms],
                  [config_property.CONFLUENT_TIER_METADATA_REPLICATION_FACTOR, metadata_replication_factor]]

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
    FETCHER_BYTES_FETCHED = TieredStorageMetric("kafka.server:type=TierFetcher", "BytesFetchedTotal")

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

    def configure_tiering(self, backend, **server_props_kwargs):
        self.kafka.jmx_object_names = [TieredStorageMetricsRegistry.ARCHIVER_LAG.mbean,
                                       TieredStorageMetricsRegistry.FETCHER_BYTES_FETCHED.mbean]
        self.kafka.jmx_attributes = [TieredStorageMetricsRegistry.ARCHIVER_LAG.attribute,
                                     TieredStorageMetricsRegistry.FETCHER_BYTES_FETCHED.attribute]
        self.kafka.server_prop_overides = tier_server_props(backend, **server_props_kwargs)

    def tiering_completed(self, topic, partitions=[0]):
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
            if last_jmx_entry.get("kafka.tier:type=TierDeletedPartitionsCoordinator,name=TierNumQueuedPartitionDeletions:Value", sys.maxint) > 0:
                return True

            if last_jmx_entry.get("kafka.tier:type=TierDeletedPartitionsCoordinator,name=TierNumInProgressPartitionDeletions:Value", sys.maxint) > 0:
                return True

        return False
