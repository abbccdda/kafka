from kafkatest.services.kafka import config_property

import sys

def tier_server_props(bucket, feature=True, enable=False, region="us-west-2", backend="S3",
                      metadata_replication_factor=3, hotset_bytes=1, hotset_ms=1,
                      log_segment_bytes=1024000, log_retention_check_interval=5000, log_roll_time = 1000):
    """Helper for building server_prop_overrides in Kafka tests that enable tiering"""
    return [
        # tiered storage does not support multiple logdirs
        [config_property.LOG_DIRS, "/mnt/kafka/kafka-data-logs-1"],
        [config_property.LOG_SEGMENT_BYTES, log_segment_bytes],
        [config_property.LOG_ROLL_TIME_MS, log_roll_time],
        [config_property.LOG_RETENTION_CHECK_INTERVAL_MS, log_retention_check_interval],
        [config_property.CONFLUENT_TIER_FEATURE, feature],
        [config_property.CONFLUENT_TIER_ENABLE, enable],
        [config_property.CONFLUENT_TIER_LOCAL_HOTSET_BYTES, hotset_bytes],
        [config_property.CONFLUENT_TIER_LOCAL_HOTSET_MS, hotset_ms],
        [config_property.CONFLUENT_TIER_METADATA_REPLICATION_FACTOR, metadata_replication_factor],
        [config_property.CONFLUENT_TIER_BACKEND, backend],
        [config_property.CONFLUENT_TIER_S3_BUCKET, bucket],
        [config_property.CONFLUENT_TIER_S3_REGION, region],
    ]

def tier_set_configs(kafka, bucket, feature=True, enable=False, region="us-west-2", backend="S3",
        metadata_replication_factor=3, hotset_bytes=1, hotset_ms=1):
    """Helper for setting tier related configs directly on kafka service. Useful if we want to modify them directly later"""
    configs = tier_server_props(bucket, feature, enable, region, backend, metadata_replication_factor, hotset_bytes, hotset_ms)
    for node in kafka.nodes:
        for config in configs:
            node.config[config[0]] = config[1]

class TierSupport():
    """Tiered storage helpers. Mix in only with KafkaService-based tests"""

    def configure_tiering(self, bucket, **server_props_kwargs):
        self.kafka.jmx_object_names = ["kafka.server:type=TierFetcher",
                                       "kafka.tier.archiver:type=TierArchiver,name=TotalLag"]
        self.kafka.jmx_attributes = ["BytesFetchedTotal", "Value"]
        self.kafka.server_prop_overides = tier_server_props(bucket, **server_props_kwargs)

    def tiering_completed(self, topic, partition=0):
        self.kafka.read_jmx_output_all_nodes()
        # N.B. for some reason the bean tag order is different in the results. Compare object names in add_log_metrics.
        log_segs_key = "kafka.log:type=Log,name=NumLogSegments,topic={},partition={}:Value".format(topic, partition)
        max_log_segments = self.kafka.maximum_jmx_value.get(log_segs_key, -1)
        if max_log_segments < 2:
            return False

        for node_stats in self.kafka.jmx_stats:
            last_jmx_entry = sorted(node_stats.items(), key=lambda kv: kv[0])[-1][1]
            last_lag = last_jmx_entry.get("kafka.tier.archiver:type=TierArchiver,name=TotalLag:Value", -1)
            last_segments = last_jmx_entry.get(log_segs_key, -1)
            if last_lag != 0 or last_segments != 1:
                return False

        return True

    def tiering_started(self, topic, partition=0):
        self.kafka.read_jmx_output_all_nodes()
        tier_size = self.kafka.maximum_jmx_value.get("kafka.log:type=Log,name=TierSize,topic={},partition={}:Value".format(topic, partition), -1)
        return tier_size > 0

    def add_log_metrics(self, topic, partitions=[0]):
        """Log-specific beans are not available at server startup"""
        names = self.kafka.jmx_object_names
        attrs = self.kafka.jmx_attributes
        names = [] if names == None else names
        attrs.append("Value") if "Value" not in attrs else attrs
        for p in partitions:
            names += ["kafka.log:name=Size,partition={},topic={},type=Log".format(p, topic),
                      "kafka.log:name=NumLogSegments,partition={},topic={},type=Log".format(p, topic),
                      "kafka.log:name=TierSize,partition={},topic={},type=Log".format(p, topic)]

    def restart_jmx_tool(self):
        for knode in self.kafka.nodes:
            knode.account.kill_java_processes(self.kafka.jmx_class_name(), clean_shutdown=False, allow_fail=True)
            idx = self.kafka.idx(knode)
            self.kafka.started[idx-1] = False
            self.kafka.start_jmx_tool(idx, knode)
