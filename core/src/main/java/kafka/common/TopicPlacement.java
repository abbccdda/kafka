/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

/*
 * Example Placement Constraint:
 * {
 *   "replicas": [
 *     {
 *       "count": 3,
 *       "constraints": {
 *         "region": "east",
 *         "zone": "b",
 *       }
 *     },
 *     {
 *       "count": 1,
 *       "constraints: {
 *         "region": "east",
 *         "zone": "c"
 *       }
 *     }
 *   ],
 *   "observers": [
 *     {
 *       "count": 2,
 *       "constraints": {
 *         "region": "west",
 *         "zone": "b"
 *       }
 *     }
 *   ]
 * }
 */

final public class TopicPlacement {
    private final int version;
    private final List<ConstraintCount> replicas;
    private final List<ConstraintCount> observers;

    @JsonCreator
    private TopicPlacement(
            @JsonProperty("version") int version,
            @JsonProperty("replicas") List<ConstraintCount> replicas,
            @JsonProperty("observers") List<ConstraintCount> observers) {

        this.version = version;
        this.replicas = replicas == null ? Collections.emptyList() : replicas;
        this.observers = observers == null ? Collections.emptyList() : observers;
    }

    public String toString() {
        return "TopicPlacement(" +
            "version=" + version +
            ",replicas=" + replicas +
            ",observers=" + observers +
            ")";
    }

    @JsonProperty(value = "version", required = true)
    public int version() {
        return version;
    }

    @JsonProperty("replicas")
    public List<ConstraintCount> replicas() {
        return replicas;
    }

    @JsonProperty("observers")
    public List<ConstraintCount> observers() {
        return observers;
    }

    /**
     * Determines if a set of attributes matches the replicas constraints.
     *
     * Returns true if any of the following is true:
     *
     * 1. Replicas constraints is empty. If the topic is configured without a replicas
     *    constraints then assume that there aren't any observers constraints. Hence,
     *    all of the brokers should be replicas.
     * 2. At least one of the replicas constraints matches the broker's attributes.
     */
    public boolean matchesReplicas(Map<String, String> attributes) {
        return replicas.isEmpty() ||
            replicas
                .stream()
                .anyMatch(constraintCount -> constraintCount.matches(attributes));
    }

    /**
     * Determines if a set of attributes matches the observers constraints.
     *
     * Returns true if at least one of the observers constraints matches the broker's
     * attributes.
     */
    public boolean matchesObservers(Map<String, String> attributes) {
        return observers
            .stream()
            .anyMatch(constraintCount -> constraintCount.matches(attributes));
    }

    private static final ObjectMapper JSON_SERDE;
    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    }

    public static TopicPlacement parse(String value) {
        if (value.trim().isEmpty()) {
            // Since we have configure the empty string "" as the default value for
            // ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG we need this
            // to succeed and return the empty value for topic placement
            return TopicPlacement.empty();
        }

        try {
            TopicPlacement topicPlacement = JSON_SERDE.readValue(value, TopicPlacement.class);

            if (topicPlacement == null) {
                throw new IllegalArgumentException("Value cannot be the JSON null: " + value);
            }

            if (topicPlacement.version() != 1) {
                throw new IllegalArgumentException(
                        "Version " + topicPlacement.version() +
                        " is not supported or the version property was not specified");
            }
            if (!topicPlacement.observers().isEmpty() && topicPlacement.replicas().isEmpty()) {
                throw new IllegalArgumentException("Replicas constraints must be specified if observers constraints are specified");
            }
            for (ConstraintCount constraint: topicPlacement.replicas) {
                if (constraint.count() == 0) {
                    throw new IllegalArgumentException("Replica constraint count cannot be zero.");
                }
            }
            for (ConstraintCount constraint: topicPlacement.observers) {
                if (constraint.count() == 0) {
                    throw new IllegalArgumentException("Observer constraint count cannot be zero.");
                }
            }

            return topicPlacement;
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception while parsing placement configuration", e);
        }
    }

    /**
     * Serialize this object back to json string. This is used to generate a compact json string
     * that gets saved as part of topic configuration.
     */
    public String toJson() {
        try {
            return JSON_SERDE.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static final TopicPlacement EMPTY = new TopicPlacement(1, null, null);

    public static TopicPlacement empty() {
        return EMPTY;
    }

    public static final class ConstraintCount {
        private final int count;
        private final Map<String, String> constraints;

        @JsonCreator
        private ConstraintCount(
                @JsonProperty("count") int count,
                @JsonProperty("constraints") Map<String, String> constraints) {
            this.count = count;
            this.constraints = constraints == null ? Collections.emptyMap() : constraints;

            this.constraints.values().removeIf(value -> value == null || value.trim().isEmpty());
        }

        @JsonProperty("count")
        public int count() {
            return count;
        }

        @JsonProperty("constraints")
        public Map<String, String> constraints() {
            return constraints;
        }

        public boolean matches(Map<String, String> attributes) {
            return constraints.entrySet().stream().allMatch(entry -> {
                return Objects.equals(entry.getValue(), attributes.get(entry.getKey()));
            });
        }

        public String toString() {
            return "ConstraintCount(" +
                "count=" + count +
                ",constraints=" + constraints +
                ")";
        }
    }

    public static final Validator VALIDATOR = new TopicPlacementValidator();

    public static final class TopicPlacementValidator implements Validator {
        private TopicPlacementValidator() {}

        @Override
        public void ensureValid(String name, Object o) {
            if (o == null) return;
            try {
                TopicPlacement.parse((String) o);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(name, o, e.getMessage());
            }
        }
    }
}
