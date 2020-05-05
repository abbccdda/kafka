SERVICE_NAME := storage-probe
IMAGE_NAME := cc-$(SERVICE_NAME)
CODECOV := true
MAIN_GO := ./main.go
GO_OUTDIR ?= bin
GO_TEST_TARGET = test-go
GO_TEST_ARGS = -race -v -cover -p=1
GO_CODECOV_TEST_ARGS = -race -v -cover -p=1
BASE_VERSION := 1.10
GO11MODULE := on
GO_BINS := main.go=storage-probe

MASTER_BRANCH := master

SOAK_TEST_NAMESPACE ?= soak-tests

DOCKER_BUILD_PRE = sync-mkinclude
.PHONY: sync-mkinclude
sync-mkinclude:
	rsync -avz --delete-after ../../mk-include/ ./.mk-include.local-context-copy

include ./mk-include/cc-begin.mk
ifeq ($(VERSION),)
include ./mk-include/cc-semver.mk
endif
include ./mk-include/cc-go.mk
include ./mk-include/cc-end.mk

$(GO_OUTDIR)/storage_probe:
	go build -o $@ -ldflags $(GO_LDFLAGS) storage_probe/main.go

