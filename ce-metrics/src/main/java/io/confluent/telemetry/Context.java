package io.confluent.telemetry;

import com.google.common.collect.ImmutableMap;

import java.util.Objects;

public class Context {

    private final ImmutableMap<String, String> labels;
    private final boolean debug;

    public Context(ImmutableMap<String, String> labels) {
        this(labels, false);
    }

    public Context(ImmutableMap<String, String> labels, boolean debug) {
        this.labels = Objects.requireNonNull(labels);
        this.debug = debug;
    }

    public ImmutableMap<String, String> labels() {
        return labels;
    }


    public boolean isDebugEnabled() {
        return debug;
    }
}