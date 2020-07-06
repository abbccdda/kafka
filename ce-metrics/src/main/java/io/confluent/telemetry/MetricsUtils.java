package io.confluent.telemetry;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetricsUtils {

    private static final String NAME_JOINER = "/";

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

    public static ZonedDateTime nowInUTC(Clock clock) {
        return Instant.now(clock).atZone(ZoneOffset.UTC);
    }

    public static ZonedDateTime nowInUTC() {
        return nowInUTC(Clock.systemUTC());
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
        return domain
                + NAME_JOINER
                + clean(group)
                + NAME_JOINER
                + clean(name);
    }

    /**
     * Converts a tag/name to match the telemetry naming conventions by converting snake_case.
     *
     * <p>
     *   Kafka metrics have tags/name in lower case separated by hyphens. Eg: total-errors
     *
     *   Yammer metrics have tags/name in upper camelcase. Eg: TotalErrors
     *
     *   Some KSQL metrics have weird casing where the metric name is a mix of upper case words
     *   separated by underscore along with lower case words separated by hyphen.
     *   Eg: PENDING_SHUTDOWN-queries
     * </p>
     * @param raw
     * @return
     */
    public static String convertCase(String raw) {
        // Special handling of KSQL metrics as we can't change these at the source.
        // https://confluentinc.atlassian.net/browse/METRICS-1833
        // PENDING_SHUTDOWN-queries => pending-shutdown-queries
        String[] nameParts = raw.split("-");
        if (nameParts.length > 1) {
            for (int index = 0; index < nameParts.length; index++) {
                nameParts[index] = UPPER_UNDERSCORE.to(LOWER_HYPHEN, nameParts[index]);
            }
            raw = String.join("-", nameParts);
        }

        // Handling of Yammer metrics
        // TotalErrors => total-errors
        String lowerHypenCase = UPPER_CAMEL.to(LOWER_HYPHEN, raw);

        // At this point all metric names are in lower hyhen format.
        // Convert them to lower underscore format and return.
        return LOWER_HYPHEN.to(LOWER_UNDERSCORE, lowerHypenCase);
    }

    // remove kafka, metric or stats as these are redundant for KafkaExporter metrics
    private final static Pattern KAFKA_METRIC_STATS1_PATTERN
            = Pattern.compile("_(kafka|metrics|stats)");
    private final static Pattern KAFKA_METRIC_STATS2_PATTERN
            = Pattern.compile("(kafka|metrics|stats)_");
    // Remove per sec from Meter as we only capture the counts and ignore the moving average rates
    private final static Pattern PER_SEC_PATTERN
            = Pattern.compile("_per_sec");
    // Special case for zookeeper
    private final static Pattern ZOOKEEPER_PATTERN
            = Pattern.compile("zoo_keeper");

    private static String clean(String raw) {
        // Convert from upper camel case to lower underscore case
        String lowerHyphenCase = convertCase(raw);
        return ZOOKEEPER_PATTERN.matcher(
                PER_SEC_PATTERN.matcher(
                        KAFKA_METRIC_STATS2_PATTERN.matcher(
                                KAFKA_METRIC_STATS1_PATTERN.matcher(lowerHyphenCase)
                                        .replaceAll(""))
                                .replaceAll(""))
                        .replaceAll(""))
                .replaceAll("zookeeper");
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
