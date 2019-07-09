package io.confluent.telemetry.serde;

import com.google.protobuf.util.JsonFormat;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtoToJson implements Deserializer {

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            Metric metric = Metric.parseFrom(data);
            return JsonFormat.printer().omittingInsignificantWhitespace().print(metric);
        } catch (Exception e) {
            String errMsg = "Error deserializing protobuf message";
            throw new SerializationException(errMsg, e);
        }

    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

}
