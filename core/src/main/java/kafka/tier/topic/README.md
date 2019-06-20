### TierMessageFormatter

Deserialize tier metadata records in the `_confluent-tier-state` topic using the console consumer and
 `kafka.tier.topic.TierMessageFormatter`.
 
##### Usage
```bash
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --from-beginning \
    --topic _confluent-tier-state \
    --formatter kafka.tier.topic.TierMessageFormatter
```

##### Example output
```
(0, 0, 2019-06-20T01:00:00.000Z): TierInitLeader(version=0, topic=<guid>-0, tierEpoch=0, messageId=<guid>, brokerId=0)
(0, 1, 2019-06-20T01:00:00.100Z): TierSegmentUploadInitiate(version=0, topicIdPartition=<guid>-0, tierEpoch=0, objectId=<guid>, baseOffset=0, endOffset=81, maxTimestamp=9034208513916568098, size=1747, hasEpochState=true, hasAbortedTxns=false, hasProducerState=true)
(0, 2, 2019-06-20T01:00:10.000Z): TierSegmentUploadComplete(version=0, topic=<guid>-0, tierEpoch=0, objectId=<guid>)
```
