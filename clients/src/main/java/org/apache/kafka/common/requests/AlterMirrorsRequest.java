/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.AlterMirrorsRequestData;
import org.apache.kafka.common.message.AlterMirrorsRequestData.OpData;
import org.apache.kafka.common.message.AlterMirrorsRequestData.StopTopicMirrorData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AlterMirrorsRequest extends AbstractRequest {

    /**
     * An alter mirror operation.
     */
    public static interface Op {
      // Empty.
    }

    public static class StopTopicMirrorOp implements Op {

        private final String topic;

        public StopTopicMirrorOp(String topic) {
            this.topic = Objects.requireNonNull(topic, "Topic not specified");
        }

        public String topic() {
            return topic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StopTopicMirrorOp that = (StopTopicMirrorOp) o;
            return Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic);
        }

        @Override
        public String toString() {
            return "StopTopicMirrorOp(topic=" + topic + ")";
        }
    }

    public static class Builder extends AbstractRequest.Builder<AlterMirrorsRequest> {

        private final AlterMirrorsRequestData data;

        public Builder(List<Op> ops, boolean validateOnly, int timeoutMs) {
            super(ApiKeys.ALTER_MIRRORS);

            List<OpData> opDatas = new ArrayList<>(ops.size());
            for (Op op : ops) {
                OpData opData = new OpData();
                if (op instanceof StopTopicMirrorOp) {
                    StopTopicMirrorOp subOp = (StopTopicMirrorOp) op;
                    opData.setStopTopicMirror(Collections.singletonList(new StopTopicMirrorData().setTopic(subOp.topic())));
                } else {
                    throw new InvalidRequestException("Unexpected mirror control op type");
                }
                opDatas.add(opData);
            }

            this.data = new AlterMirrorsRequestData()
                    .setOps(opDatas)
                    .setValidateOnly(validateOnly)
                    .setTimeoutMs(timeoutMs);
        }

        @Override
        public AlterMirrorsRequest build(short version) {
            return new AlterMirrorsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AlterMirrorsRequestData data;

    public AlterMirrorsRequest(AlterMirrorsRequestData data, short version) {
        super(ApiKeys.ALTER_MIRRORS, version);
        this.data = data;
    }

    public AlterMirrorsRequest(Struct struct, short version) {
        super(ApiKeys.ALTER_MIRRORS, version);
        this.data = new AlterMirrorsRequestData(struct, version);
    }

    public List<Op> ops() {
        List<Op> ops = new ArrayList<>(data.ops().size());
        for (OpData opData : data.ops()) {
            if (opData.stopTopicMirror() != null) {
                if (opData.stopTopicMirror().size() != 1) {
                    throw new InvalidRequestException("Unexpected request size");
                }
                StopTopicMirrorData stopTopicMirrorData = opData.stopTopicMirror().get(0);
                ops.add(new StopTopicMirrorOp(stopTopicMirrorData.topic()));
            } else {
                throw new InvalidRequestException("Unexpected mirror control op type");
            }
        }
        return ops;
    }

    public boolean validateOnly() {
        return data.validateOnly();
    }

    public int timeoutMs() {
        return data.timeoutMs();
    }

    @Override
    public AlterMirrorsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AlterMirrorsResponse(ops(), throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
