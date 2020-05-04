_docker_opts := --user $(shell id -u):$(shell id -g) --rm
_docker_opts += --volume $(CURDIR):/local --workdir /local

REDOC = docker run $(_docker_opts) confluent-docker.jfrog.io/confluentinc/redoc-cli

YAMLLINT = docker run $(_docker_opts) cytopia/yamllint
YAMLLINT_CONF := yamllint.conf.yaml

SPECCY = docker run $(_docker_opts) wework/speccy

BUILD_TARGETS += $(API_OUT_DIR)/api.html
TEST_TARGETS += api-lint

ifeq ($(HOST_OS),linux)
OPEN := xdg-open
else
OPEN := open
endif

ifeq ($(shell which $(OPEN)),)
OPEN := @echo Browse to
endif

ifndef API_OUT_DIR
	$(error API_OUT_DIR is undefined)
endif

ifndef API_SPEC
	$(error API_SPEC is undefined)
endif

API_DOC_TITLE ?= API Reference Documentation
REDOC_OPTIONS ?=

## Generate HTML documentation using ReDoc
$(API_OUT_DIR)/api.html: $(API_SPEC)
	mkdir -p $(API_OUT_DIR)
	$(REDOC) bundle /local/$(API_SPEC) \
		--output $(API_OUT_DIR)/api.html \
		--title "$(API_DOC_TITLE)" \
		--options.theme.colors.primary.main='#0074A2' \
		--options.theme.colors.primary.light='#00AFBA' \
		--options.theme.colors.primary.dark='#173361' \
		--cdn \
		$(REDOC_OPTIONS)
ifeq ($(CI),true)
ifeq ($(BRANCH_NAME),$(MASTER_BRANCH))
	artifact push project --force $(API_OUT_DIR)/api.html
else
	artifact push workflow $(API_OUT_DIR)/api.html
endif
endif

.PHONY: api-redoc-serve
## Start a ReDoc server
## This is useful for viewing the docs while iterating on spec development
api-redoc-serve: $(API_SPEC)
api-redoc-serve: _docker_opts += --init --publish 8080:8080
api-redoc-serve:
	@sleep 3 && $(OPEN) http://localhost:8080
	$(REDOC) serve $(API_SPEC) --watch

.PHONY: api-lint-yaml
## Lint the OpenAPI spec using yamllint
api-lint-yaml: $(API_SPEC)
ifneq ($(wildcard $(YAMLLINT_CONF)),)
	$(YAMLLINT) \
		-f colored \
		-c yamllint.conf.yaml \
		$(dir $(API_SPEC))
else
	$(warning Create a $(YAMLLINT_CONF) file to enable YAML linting of OpenAPI spec)
endif

.PHONY: api-lint-speccy
## Lint the OpenAPI spec using speccy
## Create a speccy.yaml file in the project root to customize config
api-lint-speccy: $(API_SPEC)
	$(SPECCY) lint $(API_SPEC)

.PHONY: api-lint
api-lint: api-lint-yaml api-lint-speccy
