#!/bin/bash

NUM_RUNS=5
NUM_RECORDS=5000000
RECORD_SIZE=100

TIMESTAMP=$(date '+%Y%m%d%H%M%S')
for ((run=1; run<=$NUM_RUNS; run++));
do
    ./bin/kafka-producer-perf-test.sh --topic my-replicated-topic --num-records $NUM_RECORDS \
    --throughput -1 --record-size $RECORD_SIZE --producer.config \
    config/raft-producer.properties >> results/kafka-num-records-${NUM_RECORDS}-record-size-${RECORD_SIZE}-"${TIMESTAMP}".out
done
