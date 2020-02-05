##########

##########

FROM openjdk:11-jdk as kafka-builder
USER root

COPY . /home/gradle

WORKDIR /home/gradle

# /root/.gradle is a docker volume so we can't copy files into it
ENV GRADLE_USER_HOME=/root/gradle-home

RUN mkdir -p /root/.m2/repository $GRADLE_USER_HOME \
  && cp ./tmp/gradle/gradle.properties $GRADLE_USER_HOME

RUN ./gradlew clean releaseTarGz -x signArchives --stacktrace -PpackageMetricsReporter=true && ./gradlew install --stacktrace

WORKDIR /build
RUN tar -xzvf /home/gradle/core/build/distributions/kafka_*-SNAPSHOT.tgz --strip-components 1

##########

# Build a Docker image for the K8s liveness storage probe.

FROM confluent-docker.jfrog.io/confluentinc/cc-service-base:1.10 AS go-build

ARG version

WORKDIR /root
COPY .ssh .ssh
COPY .netrc ./
RUN ssh-keyscan -t rsa github.com > /root/.ssh/known_hosts

WORKDIR /go/src/github.com/confluentinc/ce-kafka/cc-services/storage_probe
COPY cc-services/storage_probe .
COPY ./mk-include ./mk-include

RUN make deps DEP_ARGS=-vendor-only VERSION=${version}

RUN make lint-go test-go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build-go GO_OUTDIR= VERSION=${version}


##########

FROM confluent-docker.jfrog.io/confluentinc/cc-base:v4.1.0

ARG version
ARG confluent_version
ARG git_sha
ARG git_branch

ENV COMPONENT=kafka
ENV KAFKA_SECRETS_DIR=/mnt/secrets
ENV KAFKA_LOG4J_DIR=/mnt/log
ENV KAFKA_CONFIG_DIR=/mnt/config

EXPOSE 9092

VOLUME ["${KAFKA_SECRETS_DIR}", "${KAFKA_LOG4J_DIR}"]

LABEL git_sha="${git_sha}"
LABEL git_branch="${git_branch}"

CMD ["/opt/caas/bin/run"]

#Copy kafka
COPY --from=kafka-builder /build /opt/confluent

COPY include/opt/caas /opt/caas

# Set up storage probe
COPY --from=go-build /storage-probe /opt/caas/bin

WORKDIR /
RUN mkdir -p /opt/caas/lib \
  && curl -o /opt/caas/lib/jmx_prometheus_javaagent-0.12.0.jar -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.12.0/jmx_prometheus_javaagent-0.12.0.jar \
  && mkdir -p /opt/asyncprofiler \
  && curl -L https://github.com/jvm-profiling-tools/async-profiler/releases/download/v1.6/async-profiler-1.6-linux-x64.tar.gz | tar xz -C /opt/asyncprofiler \
  && apt update \
  && apt install -y cc-rollingupgrade-ctl=0.5.0 vim-tiny \
  && apt-get autoremove -y \
  && mkdir -p  "${KAFKA_SECRETS_DIR}" "${KAFKA_LOG4J_DIR}" /opt/caas/config/kafka \
  && ln -s "${KAFKA_CONFIG_DIR}/kafka.properties" /opt/caas/config/kafka/kafka.properties \
  && chmod -R ag+w "${KAFKA_SECRETS_DIR}" "${KAFKA_LOG4J_DIR}"
