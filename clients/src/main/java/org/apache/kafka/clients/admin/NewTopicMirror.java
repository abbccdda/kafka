/*
 * Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * Supplemental new topic data for creating a topic that is a mirror of another.
 */
@InterfaceStability.Evolving
public class NewTopicMirror {

    private final String linkName;
    private final String mirrorTopic;

    /**
     * Prepares a new topic that'll mirror its data from a source topic.
     *
     * @param linkName the name of the cluster link over which the topic will mirror from
     * @param mirrorTopic the name of the topic to be mirrored over the cluster link
     */
    public NewTopicMirror(String linkName, String mirrorTopic) {
        this.linkName = Objects.requireNonNull(linkName);
        this.mirrorTopic = Objects.requireNonNull(mirrorTopic);
    }

    /**
     * The name of the cluster link over which the topic will mirror from.
     */
    public String linkName() {
        return linkName;
    }

    /**
     * The name of the topic to be mirrored over the cluster link, i.e. the source topic's name.
     */
    public String mirrorTopic() {
        return mirrorTopic;
    }

    @Override
    public String toString() {
        return "NewTopicMirror(linkName=" + linkName + ", mirrorTopic=" + mirrorTopic + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final NewTopicMirror that = (NewTopicMirror) o;
        return Objects.equals(linkName, that.linkName) &&
            Objects.equals(mirrorTopic, that.mirrorTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(linkName, mirrorTopic);
    }
}
