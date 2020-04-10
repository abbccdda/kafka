SERVICE_NAME := tier-validator
IMAGE_NAME := cc-$(SERVICE_NAME)
RELEASE_BRANCH := master
DEFAULT_BUMP := minor

BASE_IMAGE ?= confluent-docker.jfrog.io/confluentinc/ce-kafka
# Use master clean version as docker base verson unless specified
BASE_VERSION ?= $(shell git rev-parse --is-inside-work-tree > /dev/null && git describe --tags --always master)

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-end.mk