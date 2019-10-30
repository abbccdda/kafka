#!/bin/sh
set -e 

# This is temporary init container logic that is intended to be replaced by a common init container
# https://confluentinc.atlassian.net/wiki/spaces/~roger/pages/937957745/Init+Container+Plan
encoded_host=$(/bin/cc-kafka-init encode-host)
# the emptyDir config mount shared by init container and main container is currently /mnt/config
# The sed template logic is intended to be replaced by jsonnet templating done in a common init container
cat ${KAFKA_CONFIG_DIR}/shared/server-common.properties | sed "s/{encoded_host}/${encoded_host}/g" > ${KAFKA_CONFIG_DIR}/kafka.properties
cat ${KAFKA_CONFIG_DIR}/pod/${POD_NAME}/server-pod.properties >> ${KAFKA_CONFIG_DIR}/kafka.properties
cat ${KAFKA_CONFIG_DIR}/kafka.properties
