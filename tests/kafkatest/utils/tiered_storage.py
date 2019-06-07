from kafkatest.services.kafka import config_property

def tier_server_props(bucket, feature=True, enable=False, region="us-west-2", backend="S3",
                      metadata_replication_factor=3, hotset_bytes=1):
    """Helper for building server_prop_overrides in Kafka tests that enable tiering"""
    return [
        # tiered storage does not support multiple logdirs
        [config_property.LOG_DIRS, "/mnt/kafka/kafka-data-logs-1"],
        ["confluent.tier.feature", feature],
        ["confluent.tier.enable", enable],
        ["confluent.tier.local.hotset.bytes", hotset_bytes],
        ["confluent.tier.metadata.replication.factor", metadata_replication_factor],
        ["confluent.tier.backend", backend],
        ["confluent.tier.s3.bucket", bucket],
        ["confluent.tier.s3.region", region],
    ]
