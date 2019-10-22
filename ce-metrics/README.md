# confluent-metrics

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

1. Build the jar using `./gradlew ce-metrics:build`
1. Get Apache Kafka distro, e.g. from <https://www.apache.org/dyn/closer.cgi?path=/kafka/2.3.0/kafka_2.12-2.3.0.tgz>
1. Copy `ce-metrics/build/libs/confluent-metrics-5.5.0-ce-SNAPSHOT.jar`
   into (e.g.) `kafka_2.12-2.3.0/libs/`
1. Add the following to config/server.properties in Kafka:
   ```
   metric.reporters=io.confluent.metrics.reporter.ConfluentMetricsReporter,io.confluent.telemetry.reporter.KafkaServerMetricsReporter
   confluent.metrics.reporter.bootstrap.servers=localhost:9092
   confluent.metrics.reporter.topic.replicas=1
   confluent.telemetry.exporter.kafka.producer.bootstrap.servers=localhost:9092
   confluent.telemetry.exporter.kafka.topic.replicas=1
   confluent.telemetry.labels.test=test
   ```
1. Start the broker with `bin/kafka-server-start.sh config/server.properties`.
   Metrics should be published to topics `_confluent-metrics` and `_confluent-telemetry-metrics`.
1. Read out the `_confluent-metrics` data with:
   ```
   bin/kafka-console-consumer.sh --from-beginning \
     --topic _confluent-metrics \
     --bootstrap-server localhost:9092 \
     --formatter io.confluent.metrics.reporter.ConfluentMetricsFormatter
   ```
1. Read out the `_confluent-telemetry-metrics` data with:
   ```
   bin/kafka-console-consumer.sh --from-beginning \
     --topic _confluent-telemetry-metrics \
     --bootstrap-server localhost:9092 \
     --value-deserializer io.confluent.telemetry.serde.ProtoToJson
   ```

Note: In the future, we will add instructions for testing/verifying the Producer, too.
