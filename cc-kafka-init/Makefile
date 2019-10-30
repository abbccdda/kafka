SERVICE_NAME := kafka-init
IMAGE_NAME := cc-$(SERVICE_NAME)
CHART_NAME := cc-$(SERVICE_NAME)
CODECOV := true
MAIN_GO := ./main.go
GO_OUTDIR ?= bin
GO_TEST_TARGET = test-go
GO_TEST_ARGS = -race -v -cover -p=1
GO_CODECOV_TEST_ARGS = -race -v -cover -p=1

include ../mk-include/cc-begin.mk
include ../mk-include/cc-semver.mk
include ../mk-include/cc-go.mk
include ../mk-include/cc-docker.mk
include ../mk-include/cc-end.mk
