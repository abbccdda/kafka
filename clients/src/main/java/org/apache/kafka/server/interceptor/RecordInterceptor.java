package org.apache.kafka.server.interceptor;

import java.io.Closeable;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;

public interface RecordInterceptor extends Configurable, Closeable {

    default RecordInterceptorResponse onAppend(TopicPartition tp, Record record) {
        return RecordInterceptorResponse.ACCEPT;
    }

    default void configure(Map<String, ?> configs) {
        // do nothing
    }

    default void close() {
        // do nothing
    }

    enum RecordInterceptorResponse {
        /* accept this record for appending */
        ACCEPT((byte) 0),

        /* reject this record when appending */
        REJECT((byte) 1);

        private final byte id;

        RecordInterceptorResponse(byte id) {
            this.id = id;
        }
    }
}
