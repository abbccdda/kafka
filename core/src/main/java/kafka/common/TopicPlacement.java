/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

/*
 * Example Placement Constraint:
 * {
 *   "replicas": [
 *     {
 *       "count": 2,
 *       "constraints": {
 *         "region": "east",
 *         "zone": "b",
 *       }
 *     },
 *     {
 *       "constraints: {
 *         "region": "east",
 *         "zone": "c"
 *       }
 *     }
 *   ],
 *   "observers": [
 *     {
 *       "contraints": {
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

    public boolean matchesReplicas(Map<String, String> attributes) {
        return replicas.isEmpty() ||
            replicas.stream().anyMatch(constraintCount -> constraintCount.matches(attributes));
    }

    public boolean matchesObservers(Map<String, String> attributes) {
        return observers.isEmpty() ||
            observers.stream().anyMatch(constraintCount -> constraintCount.matches(attributes));
    }

    private static final ObjectMapper JSON_SERDE;
    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    }

    public static TopicPlacement parse(String value) {
        try {
            TopicPlacement topicPlacement = JSON_SERDE.readValue(value, TopicPlacement.class);
            if (topicPlacement.version() != 1) {
                throw new IllegalArgumentException("Version " + topicPlacement.version() + " is not supported");
            }
            return topicPlacement;
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception while parsing placement configuration", e);
        }
    }

    private static final TopicPlacement EMPTY = new TopicPlacement(1, null, null);

    public static TopicPlacement empty() {
        return EMPTY;
    }

    public static final class ConstraintCount {
        private final OptionalInt count;
        private final Map<String, String> constraints;

        @JsonCreator
        private ConstraintCount(
                @JsonProperty("count") OptionalInt count,
                @JsonProperty("constraints") Map<String, String> constraints) {
            this.count = count;
            this.constraints = constraints;
        }

        @JsonProperty("count")
        public OptionalInt count() {
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

            String value = (String) o;
            if (value == null) throw new ConfigException(name, o, "Value must be a String");

            try {
                TopicPlacement.parse(value);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(name, o, e.getMessage());
            }
        }
    }
}
