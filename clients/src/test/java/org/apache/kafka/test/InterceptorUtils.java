package org.apache.kafka.test;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.interceptor.RecordInterceptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InterceptorUtils {

    public static class MockRecordInterceptor implements RecordInterceptor {

        public MockRecordInterceptor() {
            INTERCEPTED.clear();
            CONFIGURED.clear();
        }

        public static final List<Record> INTERCEPTED = new ArrayList<>();

        public static final Map<String, Object> CONFIGURED = new HashMap<>();

        @Override
        public void configure(Map<String, ?> configs) {
            CONFIGURED.putAll(configs);
        }

        @Override
        public RecordInterceptorResponse onAppend(TopicPartition tp, Record record) {
            INTERCEPTED.add(record);

            // for testing purposes
            if (ByteBuffer.wrap("reject me".getBytes()).equals(record.value()))
                return RecordInterceptorResponse.REJECT;

            return RecordInterceptorResponse.ACCEPT;
        }
    }

    public static class AnotherMockRecordInterceptor implements RecordInterceptor {

        public AnotherMockRecordInterceptor() {
            INTERCEPTED.clear();
            CONFIGURED.clear();
        }

        public static final List<Record> INTERCEPTED = new ArrayList<>();

        public static final Map<String, Object> CONFIGURED = new HashMap<>();

        @Override
        public void configure(Map<String, ?> configs) {
            CONFIGURED.putAll(configs);
        }

        @Override
        public RecordInterceptorResponse onAppend(TopicPartition tp, Record record) {
            INTERCEPTED.add(record);

            // for testing purposes
            if (ByteBuffer.wrap("reject me please".getBytes()).equals(record.value()))
                return RecordInterceptorResponse.REJECT;

            return RecordInterceptorResponse.ACCEPT;
        }
    }
}
