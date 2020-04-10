#
# Copyright Confluent Inc.
#

# Builds a Confluent Cloud Docker image for Tier Metadata Validator services

ARG base_version=latest
ARG base_image=confluent-docker.jfrog.io/confluentinc/ce-kafka

FROM $base_image:$base_version

ENV TIER_VALIDATOR_ROOT=/mnt/tier_validator
COPY scripts $TIER_VALIDATOR_ROOT/scripts
RUN chmod +x $TIER_VALIDATOR_ROOT/scripts/*.sh

# Get the kubectl binary.
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl

# Make the kubectl binary executable.
RUN chmod +x ./kubectl

# Move the kubectl executable to /usr/local/bin.
RUN mv ./kubectl /usr/local/bin

# Necessary to upload cronjob results to S3 buckets
RUN apt-get install awscli -y