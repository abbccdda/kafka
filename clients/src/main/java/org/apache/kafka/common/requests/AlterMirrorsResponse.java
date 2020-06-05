/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.AlterMirrorsResponseData;
import org.apache.kafka.common.message.AlterMirrorsResponseData.ClearTopicMirrorData;
import org.apache.kafka.common.message.AlterMirrorsResponseData.OpData;
import org.apache.kafka.common.message.AlterMirrorsResponseData.StopTopicMirrorData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class AlterMirrorsResponse extends AbstractResponse {

    /**
     * A result from an alter mirror operation.
     */
    public static interface Result {

        public static class OrError {

            private Result result;
            private ApiError error;

            public OrError(Result result) {
                this.result = result;
                this.error = ApiError.NONE;
            }

            public OrError(ApiError error) {
                this.result = null;
                this.error = error;
            }

            public Result result() {
                return result;
            }

            public ApiError error() {
                return error;
            }
        }
    }

    public static class StopTopicMirrorResult implements Result {
        // Empty.
    }

    public static class ClearTopicMirrorResult implements Result {
        // Empty.
    }

    private final AlterMirrorsResponseData data;

    public AlterMirrorsResponse(List<Result.OrError> results, int throttleTimeMs) {
        List<OpData> opDatas = new ArrayList<>(results.size());
        for (Result.OrError result : results) {
            OpData opData = new OpData()
                    .setErrorCode(result.error().error().code())
                    .setErrorMessage(result.error().message());

            if (result.result() instanceof StopTopicMirrorResult) {
                opData.setStopTopicMirror(Collections.singletonList(new StopTopicMirrorData()));
            } else if (result.result() instanceof ClearTopicMirrorResult) {
                opData.setClearTopicMirror(Collections.singletonList(new ClearTopicMirrorData()));
            } else if (result.result() != null) {
                throw new InvalidRequestException("Unexpected mirror control op type");
            }
            opDatas.add(opData);
        }

        this.data = new AlterMirrorsResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setOps(opDatas);
    }

    public AlterMirrorsResponse(Collection<AlterMirrorsRequest.Op> ops, int throttleTimeMs, Throwable e) {
        short errorCode = Errors.forException(e).code();
        String errorMessage = e.getMessage();

        List<OpData> opDatas = new ArrayList<>(ops.size());
        for (AlterMirrorsRequest.Op op : ops) {
            opDatas.add(new OpData()
                    .setErrorCode(errorCode)
                    .setErrorMessage(errorMessage));
        }

        this.data = new AlterMirrorsResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setOps(opDatas);
    }

    public AlterMirrorsResponse(Struct struct, short version) {
        this.data = new AlterMirrorsResponseData(struct, version);
    }

    public void complete(List<KafkaFutureImpl<Result>> result) {
        if (result.size() != data.ops().size()) {
            throw new IllegalArgumentException("Unexpected result size");
        }

        ListIterator<KafkaFutureImpl<Result>> iterator = result.listIterator();

        for (OpData opData : data.ops()) {
            Errors error = Errors.forCode(opData.errorCode());
            KafkaFutureImpl<Result> future = iterator.next();

            if (error != Errors.NONE) {
                future.completeExceptionally(error.exception(opData.errorMessage()));
                continue;
            }

            if ((opData.stopTopicMirror() != null ? 1 : 0) +
                (opData.clearTopicMirror() != null ? 1 : 0) != 1) {
              throw new IllegalArgumentException("Unexpected request data");
            }

            Result opResult;
            if (opData.stopTopicMirror() != null) {
                if (opData.stopTopicMirror().size() != 1) {
                    throw new IllegalArgumentException("Unexpected result size");
                }
                opResult = new StopTopicMirrorResult();
            } else if (opData.clearTopicMirror() != null) {
                if (opData.clearTopicMirror().size() != 1) {
                    throw new IllegalArgumentException("Unexpected result size");
                }
                opResult = new ClearTopicMirrorResult();
            } else {
                throw new InvalidRequestException("Unexpected mirror control op type");
            }
            future.complete(opResult);
        }
    }

    // Visible for testing.
    public AlterMirrorsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        for (OpData op : data.ops()) {
            Errors error = Errors.forCode(op.errorCode());
            counts.put(error, counts.getOrDefault(error, 0) + 1);
        }
        return counts;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }
}
