package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum RecordBatchVerifierType {
    @JsonProperty("noop")
    NOOP,
    @JsonProperty("sequentialOffsets")
    SEQUENTIAL_OFFSETS
}
