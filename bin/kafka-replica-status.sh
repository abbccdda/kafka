#!/bin/bash
# Copyright 2019 Confluent Inc.

exec $(dirname $0)/kafka-run-class.sh kafka.admin.ReplicaStatusCommand "$@"
