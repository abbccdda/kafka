#!/bin/bash
# Copyright 2020 Confluent Inc.

exec $(dirname $0)/kafka-run-class.sh kafka.admin.BrokerRemovalCommand "$@"
