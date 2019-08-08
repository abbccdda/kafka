/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.common;

import java.util.Collections;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

final public class TopicPlacementTest {
    @Test
    public void testAttributeRackMatches() {
        String json = "{\"version\":1,\"replicas\":[{\"constraints\":{\"rack\":\"abc\"}}]," +
            "\"observers\":[{\"constraints\":{\"rack\":\"def\"}}]}";
        TopicPlacement topicPlacement = TopicPlacement.parse(json);

        assertTrue(topicPlacement.matchesReplicas(Collections.singletonMap("rack", "abc")));
        assertTrue(topicPlacement.matchesObservers(Collections.singletonMap("rack", "def")));
    }

    @Test
    public void testAttributeRackMatchingEmptyConstraints() {
        String json = "{\"version\": 1}";
        TopicPlacement topicPlacement = TopicPlacement.parse(json);

        assertTrue(topicPlacement.matchesReplicas(Collections.singletonMap("rack", "abc")));
        assertFalse(topicPlacement.matchesObservers(Collections.singletonMap("rack", "def")));
    }

    @Test
    public void testAttributeRackDoesNotMatch() {
        String json = "{\"version\":1,\"replicas\":[{\"constraints\":{\"rack\":\"abc\"}}]," +
            "\"observers\":[{\"constraints\":{\"rack\":\"def\"}}]}";
        TopicPlacement topicPlacement = TopicPlacement.parse(json);

        assertFalse(topicPlacement.matchesReplicas(Collections.singletonMap("rack", "def")));
        assertFalse(topicPlacement.matchesReplicas(Collections.singletonMap("rack", "not_match")));
        assertFalse(topicPlacement.matchesReplicas(Collections.emptyMap()));
        assertFalse(topicPlacement.matchesObservers(Collections.singletonMap("rack", "abc")));
        assertFalse(topicPlacement.matchesObservers(Collections.singletonMap("rack", "not_match")));
        assertFalse(topicPlacement.matchesObservers(Collections.emptyMap()));
    }

    @Test
    public void testBasicParsingFailures() {
        String missingVersion = "{\"replicas\":[{\"constraints\":{\"rack\":\"abc\"}}]," +
            "\"observers\":[{\"constraints\":{\"rack\":\"def\"}}]}";
        assertThrows(IllegalArgumentException.class, () -> TopicPlacement.parse(missingVersion));

        String unknownFields = "{\"version\": 1, \"unknown\": \"unknown\"}";
        assertThrows(IllegalArgumentException.class, () -> TopicPlacement.parse(unknownFields));

        String unknownVersion = "{\"version\": 2}";
        assertThrows(IllegalArgumentException.class, () -> TopicPlacement.parse(unknownVersion));
    }

    @Test
    public void testValidation() {
        // Case: replicas constraints is required if observers constraint is provided
        String missingReplicaConstraint = "{\"version\":1," +
            "\"observers\":[{\"constraints\":{\"rack\":\"def\"}}]}";

        assertThrows(ConfigException.class, () -> {
            TopicPlacement.VALIDATOR.ensureValid("property.name", missingReplicaConstraint);
        });
    }
}
