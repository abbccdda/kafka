/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.common;

import java.util.Collections;
import org.apache.kafka.common.config.ConfigException;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

final public class TopicPlacementTest {
    @Test
    public void testAttributeRackMatches() {
        String json = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"abc\"}}]," +
            "\"observers\":[{\"count\": 1, \"constraints\":{\"rack\":\"def\"}}]}";
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
        String json = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"abc\"}}]," +
            "\"observers\":[{\"count\": 1, \"constraints\":{\"rack\":\"def\"}}]}";
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

    /**
     * Test a match is made for a broker that matches a rack of a constraint.
     */
    @Test
    public void testPlacementConstraintPredicateSuccess() {
        Map<String, String> replicaBroker = Collections.singletonMap("rack", "east-1");
        Map<String, String> observerBroker = Collections.singletonMap("rack", "west-1");
        String placementJson = "{\"version\": 1, " +
                "\"replicas\": [{\"count\": 2, \"constraints\": {\"rack\": \"east-1\"}}," +
                               "{\"count\": 1, \"constraints\": {\"rack\": \"east-2\"}}]," +
                "\"observers\": [{\"count\": 1, \"constraints\": {\"rack\": \"west-1\"}}]}";
        TopicPlacement topicPlacement = TopicPlacement.parse(placementJson);
        assertTrue(topicPlacement.matchesReplicas(replicaBroker));
        assertTrue(topicPlacement.matchesObservers(observerBroker));
    }

    /**
     * Test that a broker with rack is not matched to a constraint with different rack.
     */
    @Test
    public void testPlacementConstraintPredicateFailure() {
        Map<String, String> broker = Collections.singletonMap("rack", "south-1");
        String placementJson = "{\"version\": 1, " +
                "\"replicas\": [{\"count\": 2, \"constraints\": {\"rack\": \"east-1\"}}," +
                "{\"count\": 1, \"constraints\": {\"rack\": \"east-2\"}}]," +
                "\"observers\": [{\"count\": 1, \"constraints\": {\"rack\": \"west-1\"}}]}";
        TopicPlacement topicPlacement = TopicPlacement.parse(placementJson);
        assertFalse(topicPlacement.matchesReplicas(broker));
        assertFalse(topicPlacement.matchesObservers(broker));
    }

    /**
     * If no rack property is specified in broker, test that match against a constraint fails.
     */
    @Test
    public void testPlacementConstraintPredicateNoBrokerRack() {
        String placementJson = "{\"version\": 1, " +
                "\"replicas\": [{\"count\": 2, \"constraints\": {\"rack\": \"east-1\"}}," +
                "{\"count\": 1, \"constraints\": {\"rack\": \"east-2\"}}]," +
                "\"observers\": [{\"count\": 1, \"constraints\": {\"rack\": \"west-1\"}}]}";
        TopicPlacement topicPlacement = TopicPlacement.parse(placementJson);
        assertFalse(topicPlacement.matchesReplicas(Collections.emptyMap()));
        assertFalse(topicPlacement.matchesObservers(Collections.emptyMap()));
    }

    /**
     * Test that a TopicPlacement can be serialized to a JSON object and the serialization doesn't contain any
     * newline and extra spaces.
     */
    @Test
    public void testJSONSerialization() {
        String placementJson = "{\"version\": 1,%n  \"replicas\": [%n    {%n      \"count\": 2,%n      " +
                "\"constraints\": {\"rack\": \"rack-1\"}%n    }%n  ],%n  \"observers\":[%n    " +
                "{%n      \"count\": 1,%n      \"constraints\": {\"rack\": \"rack-2\"}%n    }%n  " +
                "]%n}";
        String platformIndependentJson = String.format(placementJson);
        TopicPlacement tp = TopicPlacement.parse(platformIndependentJson);
        String serializedJson = tp.toJson();
        assertTrue(serializedJson.length() < platformIndependentJson.length());
    }

    /**
     * Test that the empty string decodes to the empty/default topic placement
     */
    @Test
    public void testEmptyStringParsesToEmptyTopicPlacement() {
        assertEquals(TopicPlacement.empty(), TopicPlacement.parse(""));
    }

    /**
     * Test that parse handles a few common null cases
     */
    @Test
    public void testSuccessfulHandlingOfNullProperties() {
        String input = "{\"version\":1,\"replicas\":null,\"observers\":null}";
        TopicPlacement placement = TopicPlacement.parse(input);
        assertTrue(placement.replicas().isEmpty());
        assertTrue(placement.observers().isEmpty());

        input = "{\"version\":1,\"replicas\":[{\"count\":1,\"constraints\":null}]," +
            "\"observers\":[{\"count\":1,\"constraints\":null}]}";
        placement = TopicPlacement.parse(input);
        assertTrue(placement.replicas().stream().allMatch(constraint -> constraint.constraints().isEmpty()));
        assertTrue(placement.observers().stream().allMatch(constraint -> constraint.constraints().isEmpty()));

        input = "{\"version\":1,\"replicas\":[{\"count\":1,\"constraints\":{\"rack\":null}}]," +
            "\"observers\":[{\"count\":1,\"constraints\":{\"rack\":null}}]}";
        placement = TopicPlacement.parse(input);
        assertTrue(placement.replicas().stream().allMatch(constraint -> constraint.constraints().isEmpty()));
        assertTrue(placement.observers().stream().allMatch(constraint -> constraint.constraints().isEmpty()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingReplicaCount() {
        String placementJson = "{\"version\": 1, " +
                "\"replicas\": [{\"constraints\": {\"rack\": \"east-1\"}}," +
                "{\"constraints\": {\"rack\": \"east-2\"}}]," +
                "\"observers\": [{\"constraints\": {\"rack\": \"west-1\"}}]}";
        TopicPlacement.parse(placementJson);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingObserverCount() {
        String placementJson = "{\"version\": 1, " +
                "\"replicas\": [{\"count\": 2, \"constraints\": {\"rack\": \"east-1\"}}," +
                "{\"count\": 1, \"constraints\": {\"rack\": \"east-2\"}}]," +
                "\"observers\": [{\"constraints\": {\"rack\": \"west-1\"}}]}";
        TopicPlacement.parse(placementJson);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailsWithStringNull() {
        TopicPlacement.parse("null");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailsWithEmptyJsonObject() {
        TopicPlacement.parse("{}");
    }
}
