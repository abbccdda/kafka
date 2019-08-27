package io.confluent.telemetry;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;

import com.google.common.base.Joiner;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

public class MetricsUtils {

    private static final Joiner NAME_JOINER = Joiner.on("/");

    public static Timestamp now(Clock clock) {
        return toTimestamp(Instant.now(clock));
    }

    public static Timestamp now() {
        return now(Clock.systemUTC());
    }

    public static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }

    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    /**
     * Creates a metric name given the domain, group, and name. The new String follows the following
     * conventions and rules:
     *
     * <ul>
     *   <li>domain is expected to be a host-name like value, e.g. {@code io.confluent.kafka.server}</li>
     *   <li>group is cleaned of redundant words: "kafka," "metrics," and "stats"</li>
     *   <li>the group and name are converted to snake_case</li>
     *   <li>The name is created by joining the three components, e.g.:
     *     {@code io.confluent.kafka.producer/request_metrics/produce_request_time_ms}</li>
     * </ul>
     */
    public static String fullMetricName(String domain, String group, String name) {
        return NAME_JOINER.join(domain, clean(group), clean(name));
    }

    /**
     * Converts a tag/name to match the telemetry naming conventions by converting snake_case.
     *
     * <p>Kafka metrics have tags/name with dashes and Yammer metrics have tags/name in camelcase.</p>
     * @param raw
     * @return
     */
    public static String convertCase(String raw) {
        String lowerHypenCase = UPPER_CAMEL.to(LOWER_HYPHEN, raw);
        return LOWER_HYPHEN.to(LOWER_UNDERSCORE, lowerHypenCase);
    }

    private static String clean(String raw) {
        return
                // Convert from upper camel case to lower hypen case
                convertCase(raw)
                        //2. remove kafka, metric or stats as these are redundant for KafkaExporter metrics
                        .replaceAll("_(kafka|metrics|stats)|(kafka|metrics|stats)_", "")
                        // Remove per sec from Meter as we only capture the counts and ignore the moving average rates
                        .replaceAll("_per_sec", "")
                        // Special case for zookeeper
                        .replaceAll("zoo_keeper", "zookeeper");
    }

    /**
     * Converts the label keys to snake_case.
     *
     * @param raw the input map
     * @return the new map with keys replaced by snake_case representations.
     */
    public static Map<String, String> cleanLabelNames(Map<String, String> raw) {
        return raw.entrySet()
                .stream()
                .collect(Collectors.toMap(s -> convertCase(s.getKey()), s -> s.getValue()));
    }

}
