package org.apache.kafka.test;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.interceptor.RecordInterceptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockRecordInterceptor implements RecordInterceptor {

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

        return RecordInterceptorResponse.ACCEPT;
    }
}
