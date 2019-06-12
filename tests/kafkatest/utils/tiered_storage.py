from kafkatest.services.kafka import config_property

import sys

def tier_server_props(bucket, feature=True, enable=False, region="us-west-2", backend="S3",
                      metadata_replication_factor=3, hotset_bytes=1, hotset_ms=1):
    """Helper for building server_prop_overrides in Kafka tests that enable tiering"""
    return [
        # tiered storage does not support multiple logdirs
        [config_property.LOG_DIRS, "/mnt/kafka/kafka-data-logs-1"],
        ["confluent.tier.feature", feature],
        ["confluent.tier.enable", enable],
        ["confluent.tier.local.hotset.bytes", hotset_bytes],
        ["confluent.tier.local.hotset.ms", hotset_ms],
        ["confluent.tier.metadata.replication.factor", metadata_replication_factor],
        ["confluent.tier.backend", backend],
        ["confluent.tier.s3.bucket", bucket],
        ["confluent.tier.s3.region", region],
    ]

def archive_completed(kafka, check_max_lag):
    kafka.read_jmx_output_all_nodes()
    # if jmx_tool was restarted then we won't have the full history
    # and our lag check will not be correct
    if (check_max_lag):
        max_lag = kafka.maximum_jmx_value.get("kafka.tier.archiver:type=TierArchiver,name=TotalLag:Value", 0)
        if (max_lag < 1):
            return False

    for node_stats in kafka.jmx_stats:
        last_jmx_entry = sorted(node_stats.items(), key=lambda kv: kv[0])[-1][1]
        last_lag = last_jmx_entry.get("kafka.tier.archiver:type=TierArchiver,name=TotalLag:Value", -1)
        if last_lag > 0:
           return False 

    return True

def restart_jmx_tool(kafka):
    for knode in kafka.nodes:
        knode.account.kill_java_processes(kafka.jmx_class_name(), clean_shutdown=False, allow_fail=True)
        idx = kafka.idx(knode)
        kafka.started[idx-1] = False
        kafka.start_jmx_tool(idx, knode)
