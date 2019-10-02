// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Scope;

import java.util.Objects;

/**
 * An ID that uniquely identifies a Connect cluster in an environment that potentially contains
 * several Connect and Kafka clusters. The "scope" field is intended to provide the information
 * necessary for this identification (which currently consists of the Connect distributed group ID
 * and the ID for the Kafka cluster used for internal Connect topics); the "id" field is a
 * placeholder that may be used later on for, e.g., a Confluent Resource Name (CRN).
 */
public class ConfluentConnectClusterId {

    private final String id;
    private final Scope scope;

    public ConfluentConnectClusterId(String kafkaClusterId, String connectClusterId) {
        this("", scope(kafkaClusterId, connectClusterId));
    }

    public ConfluentConnectClusterId(@JsonProperty("id") String id, @JsonProperty("scope") Scope scope) {
        this.id = Objects.requireNonNull(id, "id may not be null");
        this.scope = Objects.requireNonNull(scope, "scope may not be null");
    }

    @JsonProperty
    public String id() {
        return id;
    }

    @JsonProperty
    public Scope scope() {
        return scope;
    }

    private static Scope scope(String kafkaClusterId, String connectClusterId) {
        return new Scope.Builder()
            .withKafkaCluster(kafkaClusterId)
            .withCluster("connect-cluster", connectClusterId)
            .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ConfluentConnectClusterId that = (ConfluentConnectClusterId) o;
        return id.equals(that.id)
            && scope.equals(that.scope);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, scope);
    }

    @Override
    public String toString() {
        return "ConfluentClusterId{" +
            "id='" + id + '\'' +
            ", scope=" + scope +
            '}';
    }
}
