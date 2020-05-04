# Confluent Cloud Makefile Includes
This is a set of Makefile include targets that are used in cloud applications.

The purpose of cc-mk-include is to present a consistent developer experience across repos/projects:
```
make deps
make build
make test
make clean
```

It also helps standardize our CI pipeline across repos:
```
make init-ci
make build
make test
make release-ci
make epilogue-ci
```

## Install
Add this repo to your repo with the command:
```shell
git subtree add --prefix mk-include git@github.com:confluentinc/cc-mk-include.git master --squash
```

To exclude these makefiles from your project language summary on GitHub, add this to your `.gitattributes`:
```
mk-include/** linguist-vendored
```

Then update your makefile like so:

### Go + Docker + Helm Service
```make
SERVICE_NAME := cc-scraper
MAIN_GO := cmd/scraper/main.go
BASE_IMAGE := golang
BASE_VERSION := 1.9

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-go.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-deployer.mk
include ./mk-include/cc-end.mk
```

### Docker + Helm Only Service
```make
IMAGE_NAME := cc-example
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/caas-base-alpine
BASE_VERSION := v0.6.1

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-deployer.mk
include ./mk-include/cc-end.mk
```

### Java (Maven) + Docker + Helm Service

#### Maven-orchestrated Docker build
```make
IMAGE_NAME := cc-java-example
CHART_NAME := cc-java-example
BUILD_DOCKER_OVERRIDE := mvn-docker-package

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-maven.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-deployer.mk
include ./mk-include/cc-end.mk
```

#### Make-orchestrated Docker build
```make
IMAGE_NAME := cc-java-example
CHART_NAME := cc-java-example
MAVEN_INSTALL_PROFILES += docker

build-docker: mvn-install

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-maven.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-deployer.mk
include ./mk-include/cc-end.mk
```

In this scenario, the `docker` profile from `io.confluent:common` is leveraged to assemble the filesystem layout
for the Docker build.  However, `cc-docker.mk` is used to invoke the actual `docker build` command.

You must also configure your project's `pom.xml` to skip the `dockerfile-maven-plugin`:
```xml
  <properties>
    <docker.skip-build>false</docker.skip-build>
  </properties>
  <profiles>
    <profile>
      <id>docker</id>
      <build>
        <plugins>
          <!--
          Skip dockerfile-maven-plugin since we do the actual docker build from make
          Note that we still leverage the `docker` profile to do the filesystem assembly
          -->
          <plugin>
            <groupId>com.spotify</groupId>
            <artifactId>dockerfile-maven-plugin</artifactId>
            <executions>
              <execution>
                <id>package</id>
                <configuration>
                  <skip>true</skip>
                </configuration>
              </execution>
            </executions>
          </plugin>
        </plugins>
      </build>
    </profile>
  </profiles>
```

### Standardized Dependencies

If you need to install a standardized dependency, just include its file.
```
include ./mk-include/cc-librdkafka.mk
include ./mk-include/cc-sops.mk
```

### OpenAPI Spec
Include `cc-api.mk` for make targets supporting OpenAPI spec development:
```
API_SPEC := src/main/resources/openapi/api.yaml
API_OUT_DIR := target

include ./mk-include/cc-api.mk
```

This will automatically integrate into the `build` and `test` top-level make targets:
* [`build` phase] Generate HTML API documentation using ReDoc
* [`test` phase] Lint the API spec using:
  * [`yamllint`](https://github.com/adrienverge/yamllint) (target: `api-lint-yaml`)
  * [`speccy`](https://github.com/wework/speccy) (target: `api-lint-speccy`)

## Updating
Once you have the make targets installed, you can update at any time by running

```shell
make update-mk-include
```

## Add github templates

To add the github PR templates to your repo

```shell
make add-github-templates
```

## Developing

If you're developing an app that uses cc-mk-include or needs to extend it, it's useful
to understand how the "library" is structured.

The consistent developer experience of `make build`, `make test`, etc. is enabled by exposing a
handful of extension points that are used internally and available for individual apps as well.
For example, when you include `cc-go.mk` it adds `clean-go` to `CLEAN_TARGETS`, `build-go` to
`BUILD_TARGETS`, and so on. Each of these imports (like `semver`, `go`, `docker`, etc) is
essentially a standardized extension.

**The ultimate effect is to be able to "mix and match" different extensions
(e.g., semver, docker, go, helm, cpd) for different applications.**

You can run `make show-args` when you're inside any given project to see what extensions
are enabled for a given standard extensible command. For example, we can see that when you
run `make build` in the `cc-scheduler-service`, it'll run `build-go`, `build-docker`, and
`helm-package`.
```
cc-scheduler-service cody$ make show-args
INIT_CI_TARGETS:      seed-local-mothership deps cpd-update gcloud-install helm-setup-ci
CLEAN_TARGETS:         clean-go clean-images clean-terraform clean-cc-system-tests helm-clean
BUILD_TARGETS:         build-go build-docker helm-package
TEST_TARGETS:          lint-go test-go test-cc-system helm-lint
RELEASE_TARGETS:      set-tf-bumped-version helm-set-bumped-version helm-add-requirements get-release-image commit-release tag-release cc-cluster-spec-service push-docker cc-umbrella-chart
RELEASE_MAKE_TARGETS:  bump-downstream-tf-consumers helm-release
CI_BIN:
```

This also shows the full list of supported extension points (`INIT_CI_TARGETS`, `CLEAN_TARGETS`, and so on).

Applications themselves may also use these extension points; for example, you can append
your custom `clean-myapp` target to `CLEAN_TARGETS` to invoke as part of `make clean`.

We also expose a small number of override points for special cases (e.g., `BUILD_DOCKER_OVERRIDE`)
but these should be rather rare.