package io.confluent.telemetry.serde;

import io.opencensus.proto.metrics.v1.Metric;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OpencensusMetricsProto implements Serde<Metric>, Serializer<Metric>, Deserializer<Metric> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Metric deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return Metric.parseFrom(data);
        } catch (SerializationException e) {
            throw e;
        } catch (Exception e) {
            String errMsg = "Error deserializing protobuf message";
            throw new SerializationException(errMsg, e);
        }
    }

    @Override
    public Metric deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public byte[] serialize(String topic, Metric data) {
        return data.toByteArray();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Metric data) {
        return serialize(topic, data);
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Metric> serializer() {
        return this;
    }

    @Override
    public Deserializer<Metric> deserializer() {
        return this;
    }


}
