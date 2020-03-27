# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

IMAGE_NAME := ce-kafka
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/cc-base
BASE_VERSION := v2.4.0
MASTER_BRANCH := master
KAFKA_VERSION := $(shell awk 'sub(/.*version=/,""){print $1}' ./gradle.properties)
VERSION_POST := -$(KAFKA_VERSION)
DOCKER_BUILD_PRE  += copy-gradle-properties
DOCKER_BUILD_POST += clean-gradle-properties

BUILD_TARGETS += build-docker-cc-kafka-init
RELEASE_POSTCOMMIT += push-docker-cc-kafka-init

ifeq ($(CONFLUENT_PLATFORM_PACKAGING),)
include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-end.mk
else
.PHONY: clean
clean:

.PHONY: distclean
distclean:

%:
	$(MAKE) -f debian/Makefile $@
endif

# Custom docker targets
.PHONY: show-docker-all
show-docker-all:
	@echo
	@echo ========================
	@echo "Docker info for ce-kafka:"
	@make VERSION=$(VERSION) show-docker
	@echo
	@echo ========================
	@echo "Docker info for cc-kafka-init"
	@make VERSION=$(VERSION) -C cc-kafka-init show-docker
	@echo
	@echo ========================
	@echo "Docker info for soak_cluster"
	@make VERSION=$(VERSION) -C cc-services/soak_cluster show-docker
	@echo
	@echo ========================
	@echo "Docker info for trogdor"
	@make VERSION=$(VERSION) -C cc-services/trogdor show-docker

.PHONY: build-docker-cc-kafka-init
build-docker-cc-kafka-init:
	make VERSION=$(VERSION) -C cc-kafka-init build-docker

.PHONY: push-docker-cc-kafka-init
push-docker-cc-kafka-init:
	make VERSION=$(VERSION) -C cc-kafka-init push-docker

.PHONY: build-docker-cc-services
build-docker-cc-services:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/soak_cluster build-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/trogdor build-docker

.PHONY: push-docker-cc-services
push-docker-cc-services:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/soak_cluster push-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/trogdor push-docker

GRADLE_TEMP = ./tmp/gradle/
.PHONY: copy-gradle-properties
copy-gradle-properties:
	mkdir -p $(GRADLE_TEMP)
	cp ~/.gradle/gradle.properties $(GRADLE_TEMP)

.PHONY: clean-gradle-properties
clean-gradle-properties:
	rm -rf $(GRADLE_TEMP)
