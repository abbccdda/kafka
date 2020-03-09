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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

public final class TopicPlacement {
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

    @Override
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

    public boolean hasObserverConstraints() {
        return !observers.isEmpty();
    }

    /**
     * Determines if a set of attributes matches the replicas constraints.
     *
     * Returns true if at least one of the replica constraints matches the broker's
     * attributes.
     */
    public boolean matchesReplicas(Map<String, String> attributes) {
        return replicas
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

    public static Optional<TopicPlacement> parse(String value) {
        if (value == null || value.trim().isEmpty()) {
            // Since we have configure the empty string "" as the default value for
            // ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG we need this
            // to succeed and return the empty value for topic placement
            return Optional.empty();
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
            if (topicPlacement.replicas().isEmpty()) {
                throw new IllegalArgumentException("At least one replica constraint must be specified.");
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

            return Optional.of(topicPlacement);
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
            return constraints.entrySet().stream().allMatch(entry ->
                Objects.equals(entry.getValue(), attributes.get(entry.getKey()))
            );
        }

        @Override
        public String toString() {
            return "ConstraintCount(" +
                "count=" + count +
                ",constraints=" + constraints +
                ")";
        }

        public static ConstraintCount of(int count, Map<String, String> constraint) {
            return new ConstraintCount(count, constraint);
        }
    }

    public static final class Replica {
        private final int id;
        private final Optional<Map<String, String>> attributes;

        private Replica(int id, Optional<Map<String, String>> attributes) {
            this.id = id;
            this.attributes = attributes;
        }

        public int id() {
            return id;
        }

        public Optional<Map<String, String>> attributes() {
            return attributes;
        }

        @Override
        public String toString() {
            return "Replica(" +
                "id=" + id +
                ",attributes=" + attributes +
                ")";
        }

        /**
         * Returns a Replica from the id and set of attributes. This type is used to match assignments
         * against TopicPlacement constraints.
         *
         * @param id The id of the replica
         * @param attributes The attributes for the replica.
         *                   1. If the value is Optional.empty() then it is expected to match any constraint. This
         *                      is useful for offline observers since it is safe to allow them to match any constraint.
         *                   2. If the value is Optiona.of(Map.of()) then it is expected to not match any constraint.
         *                   3. Otherwise the value is used to match one of the constraints.
         */
        public static Replica of(int id, Optional<Map<String, String>> attributes) {
            return new Replica(id, attributes);
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

    /**
     * Returns an error string if the sync and observer replica assignment doesn't match the constraints.
     * This algorithm assumes that replicas with attributes.isPresent() only match one of the constraints.
     * See {@link Replica} for a description of the type.
     *
     * @param topicPlacement The replica placement constraint to use to validate the assignment.
     * @param syncReplicas The list sync replicas in the assignment. See {@link Replica}.
     * @param observers The list observer replicas in the assignment. See {@link Replica}.
     */
    public static Optional<String> validateAssignment(
            TopicPlacement topicPlacement,
            List<Replica> syncReplicas,
            List<Replica> observers) {
        Optional<String> syncMessage = matchesConstraints(topicPlacement.replicas(), syncReplicas)
            .map(message -> String.format(message, "sync replicas"));

        if (syncMessage.isPresent()) return syncMessage;

        return matchesConstraints(topicPlacement.observers(), observers)
                    .map(message -> String.format(message, "observers"));
    }

    /**
     * Returns an error string if the replicas don't match the constraints. This
     * algorithm assumes that replicas with attributes.isPresent() only match one
     * of the constraints.
     *
     * @param constraints List of constraints to use to determine if the replicas match.
     * @param replicas List of replica ids and their attributes. See {@link Replica}.
     */
    private static Optional<String> matchesConstraints(
            List<ConstraintCount> constraints,
            List<Replica> replicas) {

        final List<Replica> invalidReplicas = new ArrayList<>();
        List<ConstraintCount> pendingConstraints = new ArrayList<>(constraints);

        /* Sort the replicas to make sure that replicas with attribute of Optional.of(...) are
         * compared first against the constraints.
         */
        final List<Replica> sortedReplicas = new ArrayList<>(replicas);
        sortedReplicas.sort(Comparator.comparingInt(replica -> {
            return replica.attributes().isPresent() ? 0 : 1;
        }));

        for (Replica replica : sortedReplicas) {
            boolean matched = false;
            final List<ConstraintCount> currentConstraints = pendingConstraints;
            pendingConstraints = new ArrayList<>();

            // Find the matching constraint and decrease the count
            for (ConstraintCount constraint : currentConstraints) {
                if (matched) {
                    pendingConstraints.add(constraint);
                } else {
                    matched = replica
                        .attributes()
                        .map(Stream::of)
                        .orElse(Stream.empty())
                        .allMatch(attributes -> constraint.matches(attributes));
                    if (matched) {
                        /* Attributes matched the constraint or it was Optional.empty() which
                         * matches any constraint. Decrease the count if it is greater than 1 else
                         * remove the constraint.
                         */
                        if (constraint.count() > 1) {
                            pendingConstraints.add(
                                    ConstraintCount.of(
                                        constraint.count() - 1,
                                        constraint.constraints));
                        }
                    } else {
                        pendingConstraints.add(constraint);
                    }
                }
            }

            if (!matched) {
                invalidReplicas.add(replica);
            }
        }

        return generateError(constraints, replicas, invalidReplicas, pendingConstraints);
    }

    private static Optional<String> generateError(
            List<ConstraintCount> constraints,
            List<Replica> replicas,
            List<Replica> invalidReplicas,
            List<ConstraintCount> pendingConstraints) {

        Optional<String> message = Optional.empty();
        if (invalidReplicas.isEmpty() && !pendingConstraints.isEmpty()) {
            List<Integer> replicaIds = replicas
                .stream()
                .map(replica -> replica.id())
                .collect(Collectors.toList());

            int constraintSum = constraints
                .stream()
                .mapToInt(constraint -> constraint.count())
                .sum();
            message = Optional.of(
                String.format(
                    "Number of assigned replicas (%s) doesn't match the %%s constraint counts %s",
                    replicaIds,
                    constraintSum
                )
            );
        } else if (!invalidReplicas.isEmpty() || !pendingConstraints.isEmpty()) {
            List<Integer> invalidReplicaIds = invalidReplicas
                .stream()
                .map(replica -> replica.id())
                .collect(Collectors.toList());


            Set<Integer> matchingReplicaIds = replicas
                .stream()
                .map(replica -> replica.id())
                .collect(Collectors.toSet());
            matchingReplicaIds.removeAll(new HashSet<>(invalidReplicaIds));

            message = Optional.of(
                String.format(
                    "Replicas (%s) do not match the %%s constraints (%s). The following replicas matched: %s.",
                    invalidReplicaIds,
                    constraints,
                    matchingReplicaIds
                )
            );
        }

        return message;
    }
}
