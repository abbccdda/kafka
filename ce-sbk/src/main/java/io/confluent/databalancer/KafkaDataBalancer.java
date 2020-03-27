/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import kafka.controller.DataBalancer;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaDataBalancer implements DataBalancer {
    private KafkaConfig kafkaConfig;
    private KafkaCruiseControl cruiseControl = null;

    private final static String START_ANCHOR = "^";
    private final static String END_ANCHOR = "$";
    private final static String WILDCARD_SUFFIX = ".*";

    public KafkaDataBalancer(KafkaConfig config) {
        Objects.requireNonNull(config, "KafkaConfig must not be null");
        kafkaConfig = config;
    }

    /**
     * Visible for testing. cruiseControl expected to be a mock testing object
     */
    KafkaDataBalancer(KafkaConfig kafkaConfig, KafkaCruiseControl cruiseControl) {
        this(kafkaConfig);
        this.cruiseControl = cruiseControl;
    }

    @Override
    public void startUp() {

    }

    @Override
    public void shutdown() {

    }

    /**
     * Updates the internal cruiseControl configuration based on dynamic property updates in the broker's KafkaConfig
     */
    @Override
    public synchronized void updateConfig(KafkaConfig newConfig) {
        if (cruiseControl == null) {
            // cruise control isn't currently running, updated config values will be loaded in once cruise control starts.
            // at this point the singleton kafkaConfig object has already been updated, if cruise control starts at any point
            // after updateConfig has been called, it will read from the updated kafkaConfig
            kafkaConfig = newConfig;
            return;
        }
        if (kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG) != newConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG)) {
            cruiseControl.setSelfHealingFor(AnomalyType.GOAL_VIOLATION, newConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG));
        }
        if (kafkaConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG) != newConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG)) {
            cruiseControl.updateThrottle(newConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG));
        }
        kafkaConfig = newConfig;
    }

    /**
     * The function forms a regex expression by performing OR operation on the topic names and topic prefixes
     * considering each of these as string literals.
     * For example,
     * confluent.balancer.exclude.topic.names = [topic1, topic2],
     * confluent.balancer.exclude.topic.prefixes = [prefix1, prefix2]
     * The regex computed would be = "^\\Qtopic1\\E$|^\\Qtopic2\\E$|^\\Qprefix1\\E.*|^\\Qprefix2\\E.*"
     * Visible for testing
     */
    String generateRegex(KafkaConfig config) {
        List<String> topicNames = config.getList(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_NAMES_CONFIG);
        List<String> topicPrefixes = config.getList(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_PREFIXES_CONFIG);

        Stream<String> topicNameRegexStream = topicNames.stream().map(topic -> START_ANCHOR + Pattern.quote(topic) + END_ANCHOR);
        Stream<String> topicPrefixRegexStream = topicPrefixes.stream().map(prefix -> START_ANCHOR + Pattern.quote(prefix) + WILDCARD_SUFFIX);

        return Stream.concat(topicNameRegexStream, topicPrefixRegexStream).collect(Collectors.joining("|"));
    }
}
