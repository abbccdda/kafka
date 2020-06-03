#!/bin/bash

NUM_RUNS=5
NUM_RECORDS=5000000
RECORD_SIZE=1000

for ((run=1; run<=$NUM_RUNS; run++));
do
    ./bin/kafka-producer-perf-test.sh --topic __cluster_metadata --num-records $NUM_RECORDS --throughput -1 --record-size $RECORD_SIZE --producer.config config/raft-producer.properties >> raft-fsync-num-records-${NUM_RECORDS}-record-size-${RECORD_SIZE}.out
done
