<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [confluent-metrics](#confluent-metrics)
  - [Local testing](#local-testing)
    - [Sending Metrics to kafka locally](#sending-metrics-to-kafka-locally)
      - [Build and deploy a distribution](#build-and-deploy-a-distribution)
      - [Create a local `server.properties` file](#create-a-local-serverproperties-file)
      - [Start a local Zookeeper](#start-a-local-zookeeper)
      - [Start Kafka broker](#start-kafka-broker)
      - [Read metrics](#read-metrics)
    - [Sending Metrics to Sandbox Environment](#sending-metrics-to-sandbox-environment)
      - [Build and deploy a distribution](#build-and-deploy-a-distribution-1)
      - [Create a local `server.properties` file](#create-a-local-serverproperties-file-1)
        - [Using Kafka Exporter](#using-kafka-exporter)
        - [Using HTTP Exporter](#using-http-exporter)
      - [Start a local Zookeeper](#start-a-local-zookeeper-1)
      - [Start Kafka broker](#start-kafka-broker-1)
      - [Read Metrics from Sandbox Environment](#read-metrics-from-sandbox-environment)
  - [Testing in CPD](#testing-in-cpd)
    - [Sending Metrics to Sandbox Environment from CPD](#sending-metrics-to-sandbox-environment-from-cpd)
      - [Using Kafka Exporter](#using-kafka-exporter-1)
      - [Using Http Exporter](#using-http-exporter)
      - [Read Metrics from Sandbox Environment](#read-metrics-from-sandbox-environment-1)
    - [Sending Metrics to an LKC in CPD](#sending-metrics-to-an-lkc-in-cpd)
  - [Shadow JAR notes](#shadow-jar-notes)
    - [Gradle Shadow Plugin Configuration](#gradle-shadow-plugin-configuration)
    - [Verifying the contents of the shaded jar](#verifying-the-contents-of-the-shaded-jar)
    - [Manual testing of uber jar](#manual-testing-of-uber-jar)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# confluent-metrics

## Local testing

There are 2 ways to perform local testing
1. [Publish metrics to kafka locally and consume it using kafka consumer](#sending-metrics-to-kafka-locally)
2. [Publish metrics to sandbox environment and consume it from sandbox Druid/Metrics API](#sending-metrics-to-sandbox-environment)

Steps for both these options are explained below.

### Sending Metrics to kafka locally

#### Build and deploy a distribution
From the root of your `ce-kafka` workspace:
```shell
$ rm -rf core/build/distributions \
   && ./gradlew -PpackageMetricsReporter=true clean build releaseTarGz \
      -x test -x checkstyleMain -x checkstyleTest -x spotbugsMain

$ rm -rf /tmp/kafka* ; tar -xf core/build/distributions/kafka_*-SNAPSHOT.tgz --directory /tmp
```

You should now have a kafka distribution installed in `/tmp` (e.g. `/tmp/kafka_2.12-6.0.0-ce-SNAPSHOT`).
> TIP: You can refer to this directory using a wildcard `/tmp/kafka_*` for a more reusable command history.

#### Create a local `server.properties` file
Create a local `server.properties` file with contents similar to the following
```ini
##################### Miscellaneous #######################
transaction.state.log.replication.factor=1
offsets.topic.replication.factor=1
confluent.license.topic.replication.factor=1
confluent.metadata.topic.replication.factor=1
log.dirs=/tmp/kafka/data
zookeeper.connect=localhost:2181

##################### Telemetry Reporter #######################
metric.reporters=io.confluent.telemetry.reporter.TelemetryReporter

confluent.telemetry.labels.kafka.physical_cluster_id=pkc-foo

confluent.telemetry.exporter._confluent.enabled=false

confluent.telemetry.exporter._local.enabled=true
confluent.telemetry.exporter._local.topic.replicas=1

confluent.telemetry.debug.enabled=true
```

To also test the legacy `ConfluentMetricsReporter` include the following in your `server.properties`:
```ini
   metric.reporters=io.confluent.telemetry.reporter.TelemetryReporter,io.confluent.metrics.reporter.ConfluentMetricsReporter
   confluent.metrics.reporter.bootstrap.servers=localhost:9092
   confluent.metrics.reporter.topic.replicas=1
```

#### Start a local Zookeeper
You can run Zookeeper from a local [Confluent Platform](https://www.confluent.io/download) install:
```shell
$ /opt/confluent-5.3.1/bin/zookeeper-server-start
```

Or using docker:
```shell
$ docker run --env ZOOKEEPER_CLIENT_PORT=2181 --env ZOOKEEPER_TICK_TIME=2000 --publish 2181:2181 confluentinc/cp-zookeeper
```

#### Start Kafka broker
Finally, start the kafka broker:
```shell
$ LOG_DIR=/tmp/kafka/logs /tmp/kafka_*/bin/kafka-server-start /path/to/your/server.properties
```

#### Read metrics
Read out the `_confluent-telemetry-metrics` data with:

```
$ /tmp/kafka_*/bin/kafka-console-consumer.sh --from-beginning \
   --topic _confluent-telemetry-metrics \
   --bootstrap-server localhost:9092 \
   --value-deserializer io.confluent.telemetry.serde.ProtoToJson
```

If you've also enabled the legacy `ConfluentMetricsReporter` read the `_confluent-metrics` data with:

```
$ ./bin/kafka-console-consumer.sh --from-beginning \
   --topic _confluent-metrics \
   --bootstrap-server localhost:9092 \
   --formatter io.confluent.metrics.reporter.ConfluentMetricsFormatter
```

### Sending Metrics to Sandbox Environment

#### [Build and deploy a distribution](#build-and-deploy-a-distribution)

#### Create a local `server.properties` file
There are 2 ways to publish metrics to our sandbox environment:
1. Export metrics directly to sandbox kafka using the kafka metrics exporter
2. Export metrics to sandbox http endpoint using http exporter

##### Using Kafka Exporter

Use the following `server.properties` to send metrics to our sandbox kafka using the kafka exporter
```
# Telemetry Reporter
metric.reporters=io.confluent.telemetry.reporter.TelemetryReporter
confluent.telemetry.exporter._confluent.enabled=false
confluent.telemetry.exporter._local.enabled=false
confluent.telemetry.exporter.kafka.type=kafka
confluent.telemetry.exporter.kafka.enabled=true
confluent.telemetry.exporter.kafka.producer.ssl.endpoint.identification.algorithm=https
confluent.telemetry.exporter.kafka.producer.sasl.mechanism=PLAIN
confluent.telemetry.exporter.kafka.producer.request.timeout.ms=20000
confluent.telemetry.exporter.kafka.producer.bootstrap.servers=pkc-43k0e.us-west-2.aws.confluent.cloud:9092
confluent.telemetry.exporter.kafka.producer.retry.backoff.ms=500
confluent.telemetry.exporter.kafka.producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
   username="P7O4P4YF4VGRQPWA" \
   password="<GET FROM LASTPASS: Shared-Observability/Sandbox metrics lkc-l9rd5>";
confluent.telemetry.exporter.kafka.producer.security.protocol=SASL_SSL
```

##### Using HTTP Exporter

Use the following `server.properties` to send metrics to sandbox HTTP endpoint using http exporter
```
# Telemetry Reporter
metric.reporters=io.confluent.telemetry.reporter.TelemetryReporter
confluent.telemetry.exporter._local.enabled=false
confluent.telemetry.exporter._confluent.enabled=false
confluent.telemetry.exporter.http.type=http
confluent.telemetry.exporter.http.enabled=true
confluent.telemetry.exporter.http.client.base.url=https://devel-sandbox-collector.telemetry.aws.confluent.cloud
confluent.telemetry.exporter.http.api.key=<valid-devel-cloud-login-username>
confluent.telemetry.exporter.http.api.secret=<valid-devel-cloud-login-password>
confluent.telemetry.debug.enabled=true
```

#### [Start a local Zookeeper](#start-a-local-zookeeper)

#### [Start Kafka broker](#start-kafka-broker)

#### Read Metrics from Sandbox Environment
 
These metrics will then be accessible via the following backends:
* Druid: https://druid-preprod.telemetry.aws.confluent.cloud:8888/
* Metrics API: https://devel-sandbox-api.telemetry.confluent.cloud (Currently Metrics API only serves data pushed using Kafka Exporter. Data published to http endpoint can be accessed only from Druid)

## Testing in CPD

There are 2 ways to perform testing in CPD
1. [Publish metrics to sandbox environment and consume it from sandbox Druid/Metrics API](#sending-metrics-to-sandbox-environment-from-cpd)
2. [Publish metrics to an LKC running within CPD and consume it using kafka console consumer](#sending-metrics-to-an-lkc-in-cpd)

Steps for both these options are explained below.

### Sending Metrics to Sandbox Environment from CPD
There are 2 ways to publish metrics to our sandbox environment:
1. Export metrics directly to sandbox kafka using the kafka metrics exporter
2. Export metrics to sandbox http endpoint using http exporter

#### Using Kafka Exporter

By default the Telemetry Reporter running in CPD sends metrics to our sandbox kafka using the kafka exporter.

#### Using Http Exporter

Manually edit the config map of the PKC running in the CPD...
```
   $ kubectl -n pkc-foobar edit configmap kafka-shared-config
```
...and enable the telemetry reporter
    
```
  data:
    server-common.properties: |
      ...

      # Telemetry Reporter
      metric.reporters=io.confluent.telemetry.reporter.TelemetryReporter
      confluent.telemetry.exporter._local.enabled=false
      confluent.telemetry.exporter._confluent.enabled=false
      confluent.telemetry.exporter.http.type=http
      confluent.telemetry.exporter.http.enabled=true
      confluent.telemetry.exporter.http.client.base.url=https://devel-sandbox-collector.telemetry.aws.confluent.cloud
      confluent.telemetry.exporter.http.api.key=<valid-devel-cloud-login-username>
      confluent.telemetry.exporter.http.api.secret=<valid-devel-cloud-login-password>
      confluent.telemetry.debug.enabled=true
```
Restart the `kafka-0` pod
   ```
   $ kubectl -n pkc-foobar delete pod kafka-0
   ```

#### [Read Metrics from Sandbox Environment](#read-metrics-from-sandbox-environment) 

### Sending Metrics to an LKC in CPD

Kafka clusters provisioned within [CPD](https://github.com/confluentinc/cpd) environments send Telemetry data to our sandbox kafka by default(which can then be consumed from Druid/Metrics API).
But as a part of testing, if you want to publish metrics to one of the LKCs running within CPD, it can be achieved with the following instructions.

1. Follow the in the [CPD README](https://github.com/confluentinc/cpd) to create a CPD
   environment and provision a Kafka cluster within that environment.  You should have
   two Kafka clusters created at this point.

   ```
   $ ccloud kafka cluster list
          Id       |     Name     | Provider |   Region    | Durability | Status
   +---------------+--------------+----------+-------------+------------+--------+
       lkc-xzr3or7 | Production   | gcp      | us-central1 | LOW        | UP
       lkc-yxp5qp9 | Professional | gcp      | us-central1 | LOW        | UP
    ```

1. Create an API key for one of the LKCs
   ```
   $ ccloud kafka cluster use lkc-yxp5qp9
   $ ccloud api-key create
   Save the API key and secret. The secret is not retrievable later.
   +---------+------------------------------------------------------------------+
   | API Key | 6M3P6NEBYCS4UDAN                                                 |
   | Secret  | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx                                |
   +---------+------------------------------------------------------------------+
   ```

1. Determine the PKC name for the other Kafka cluster (`pkc-21vx0v4` in this example)
   ```
   $ ccloud kafka cluster describe lkc-xzr3or7 | grep Endpoint
   | Endpoint    | SASL_SSL://pkc-21vx0v4.us-central1.gcp.priv.cpdev.cloud:9092 |
   | ApiEndpoint | https://pkac-xzr3or7.us-central1.gcp.priv.cpdev.cloud        |
   ```

1. Manually edit the configmap for that cluster...
   ```
   $ kubectl -n pkc-21vx0v4 edit configmap kafka-shared-config
   ```
   ...and enable the telemetry reporter
   ```yaml
   data:
     server-common.properties: |
       ...

       # Telemetry Reporter
       metric.reporters=io.confluent.telemetry.reporter.TelemetryReporter
       confluent.telemetry.exporter._confluent.enabled=false
       confluent.telemetry.exporter._local.enabled=false
       confluent.telemetry.exporter.kafka.type=kafka
       confluent.telemetry.exporter.kafka.enabled=true
       confluent.telemetry.exporter.kafka.topic.replicas=3
       confluent.telemetry.exporter.kafka.topic.name=telemetry
       confluent.telemetry.exporter.kafka.topic.max.message.bytes=8388608
       confluent.telemetry.exporter.kafka.producer.ssl.endpoint.identification.algorithm=https
       confluent.telemetry.exporter.kafka.producer.sasl.mechanism=PLAIN
       confluent.telemetry.exporter.kafka.producer.request.timeout.ms=20000
       confluent.telemetry.exporter.kafka.producer.bootstrap.servers=pkc-empj6vn.us-central1.gcp.priv.cpdev.cloud:9092
       confluent.telemetry.exporter.kafka.producer.retry.backoff.ms=500
       confluent.telemetry.exporter.kafka.producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
          username="6M3P6NEBYCS4UDAN" \
          password="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
       confluent.telemetry.exporter.kafka.producer.security.protocol=SASL_SSL
       confluent.telemetry.debug.enabled=true
   ```
   > NOTE: We set `confluent.telemetry.exporter.kafka.topic.name=telemetry` so that the
   > topic shows up in the Confluent Cloud UI and CLI (the default name uses the `_` prefix
   > which hides the topic)

1. Restart the `kafka-0` pod
   ```
   $ kubectl -n pkc-21vx0v4 delete pod kafka-0
   ```

Now you can inspect the metrics protobuf messages using the `ccloud` and `kafka-*` CLIs

1. Verify that the `telemetry` topic was created
   ```
   $ ccloud api-key use 6M3P6NEBYCS4UDAN
   $ ccloud kafka topic list
       Name
   +-----------+
     telemetry
   ```
1. Read the metric protobuf messages as JSON
   > Note we use the `kafka-console-consumer` here for the ProtoToJson deserializer
   ```
   $ kafka-console-consumer \
      --bootstrap-server pkc-empj6vn.us-central1.gcp.priv.cpdev.cloud:9092 \
      --consumer.config ./consumer.properties \
      --topic telemetry \
      --from-beginning \
      --value-deserializer io.confluent.telemetry.serde.ProtoToJson \
      | head -1
   {"metricDescriptor":{"name":"io.confluent.kafka.server/request/local_time_ms/time/delta","type":"GAUGE_DOUBLE","labelKeys":[{"key":"kafka.cluster.id"},{"key":"request"},{"key":"cluster_id"},{"key":"library"},{"key":"java.version"},{"key":"broker_id"},{"key":"java.version.extended"},{"key":"kafka.id"},{"key":"host.hostname"},{"key":"kafka.broker.id"},{"key":"metric_name_original"},{"key":"kafka.version"}]},"timeseries":[{"startTimestamp":"2020-02-20T22:58:34.273564Z","labelValues":[{"value":"ut53nsiMSaem1vKZ6XVvQQ"},{"value":"LeaveGroup"},{"value":"ut53nsiMSaem1vKZ6XVvQQ"},{"value":"yammer"},{"value":"11.0.5"},{"value":"0"},{"value":"11.0.5+10"},{"value":"ut53nsiMSaem1vKZ6XVvQQ"},{"value":"kafka-0"},{"value":"0"},{"value":"kafka.network:RequestMetrics:LocalTimeMs"},{"value":"5.5.0-ce-SNAPSHOT"}],"points":[{"timestamp":"2020-02-20T22:58:50.952301Z","doubleValue":0.0}]}],"resource":{"type":"kafka","labels":{"java.version":"11.0.5","java.version.extended":"11.0.5+10","host.hostname":"kafka-0","kafka.version":"5.5.0-ce-SNAPSHOT","kafka.id":"ut53nsiMSaem1vKZ6XVvQQ","kafka.cluster.id":"ut53nsiMSaem1vKZ6XVvQQ","kafka.broker.id":"0","cluster_id":"ut53nsiMSaem1vKZ6XVvQQ","broker_id":"0"}}}
   ```

## Shadow JAR notes

The shaded jar for `confluent-metrics` is meant to be mostly standalone. Namely,
we intend for either of the MetricsReporter implementations it provides to work
with a vanilla build of Apache Kafka. To that end, we build a shaded and
relocated JAR that bundles all dependencies except for those that are already
provided by Kafka and we guaranteed to have at runtime. The shaded/relocated
jar helps to avoid dependency conflicts for any jars added by the user.

### Gradle Shadow Plugin Configuration

Our configuration of the Gradle shadow plugin has a number of nuances. These
are:

1. We specify that Yammer metrics, slf4j, kafka-clients, and kafka are all
   `shadow` deps. The `shadow` configuration is a feature of the [shadow gradle
   plugin](https://imperceptiblethoughts.com/shadow/configuration/#configuring-the-runtime-classpath).
   This means that they are _not_ included in the shaded jar, and they are
   marked as "runtime" dependencies in the pom that we build.
2. The shadow jar is configured to bundle all other dependencies and their
   transitive dependencies. We relocate anything that is not `io.confluent` to
   `io.confluent.shaded`. If you add a new dependency, you may need to add a new
   relocation rule under `shadowJar{}`.
3. During the releaseTarGz bundling, we only copy the shaded jar itself into the
   final artifact. Ideally, we would also copy in the `shadow` dependencies, but
   we don't due to a nuance of how that is setup. Since all of the dependencies
   are provided by subprojects, they end up in the release artifact anyway.
4. We modify the pom that is published (see `uploadArchives{}` under the
   `ce-metrics` project) to exclude all compile and runtime deps (these are put
   into the shaded jar), and to mark the dependencies that are `shadow` as
   runtime. This is based off of the `uploadShadow{}` artifact. We do it via
   `uploadArchives{}` so that artifacts can be signed.

### Verifying the contents of the shaded jar

You can verify that all classes have
been relocated with:

```shell
$ jar tf ce-metrics/build/libs/confluent-metrics-5.5.0-ce-SNAPSHOT.jar | grep -E -v "^io/confluent" | grep class
# should return no matches
```

### Manual testing of uber jar
(this is to be added to muckrake as an automated test)

Follow the [Local testing](#local-testing) section above, with the following modifications:

1. Get Apache Kafka distro, e.g. from <https://www.apache.org/dyn/closer.cgi?path=/kafka/2.3.0/kafka_2.12-2.3.0.tgz>
1. Copy `ce-metrics/build/libs/confluent-metrics-5.5.0-ce-SNAPSHOT.jar`
   into (e.g.) `kafka_2.12-2.3.0/libs/`
1. Be sure to enable both metric reporters in `server.properties`

Note: In the future, we will add instructions for testing/verifying the Producer, too.
