##########

##########

FROM gradle:5.1.1-jdk11 as kafka-builder
USER root

COPY . /home/gradle

WORKDIR /home/gradle

RUN mkdir -p /home/gradle/.m2/repository

RUN gradle && ./gradlew clean releaseTarGz -x signArchives --stacktrace -PpackageMetricsReporter=true && ./gradlew install --stacktrace

WORKDIR /build
RUN tar -xzvf /home/gradle/core/build/distributions/kafka_*-SNAPSHOT.tgz --strip-components 1

##########

##########

FROM confluent-docker.jfrog.io/confluentinc/cc-base:v3.2.0

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

WORKDIR /
RUN mkdir -p /opt/caas/lib \
  && curl -o /opt/caas/lib/jmx_prometheus_javaagent-0.1.0.jar -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.1.0/jmx_prometheus_javaagent-0.1.0.jar \
  && apt update \
  && apt install -y cc-rollingupgrade-ctl=0.5.0 vim-tiny \
  && apt-get autoremove -y \
  && mkdir -p  "${KAFKA_SECRETS_DIR}" "${KAFKA_LOG4J_DIR}" /opt/caas/config/kafka \
  && chmod -R ag+w "${KAFKA_SECRETS_DIR}" "${KAFKA_LOG4J_DIR}"
