package io.confluent.telemetry.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.telemetry.MetricsUtils;
import io.opencensus.proto.metrics.v1.DistributionValue;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.SummaryValue;
import io.opencensus.proto.metrics.v1.TimeSeries;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Implements a Deserializer that flattens the Metric (assuming one point and one time series)
 * before writing it out a JSON string.
 *
 * <p>This method is typically used for debugging.</p>
 */
public class ProtoToFlatJson implements Deserializer<String> {
    private static final String TIMESTAMP_COLUMN = "timestamp";
    private static final String NAME = "name";
    private static final String TYPE = "type";

    private ObjectMapper mapper = new ObjectMapper();

    public ProtoToFlatJson() {
        this(false);
    }

    public ProtoToFlatJson(boolean orderByKeys) {
        if (orderByKeys) {
            mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        }
    }

    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public void close() {

    }

    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Metric metric = Metric.parseFrom(data);

            // Flatten out the OpenCensus record
            for (TimeSeries ts : metric.getTimeseriesList()) {

                HashMap<String, Object> labels = new HashMap<>();

                // Add labels to record.
                for (int i = 0; i < metric.getMetricDescriptor().getLabelKeysCount(); i++) {
                    labels.put(metric.getMetricDescriptor().getLabelKeys(i).getKey(), ts.getLabelValues(i).getValue());
                }

                labels.put(TYPE, metric.getMetricDescriptor().getType().toString());
                // One row per timeseries- point.
                for (Point point : ts.getPointsList()) {
                    // Time in millis
                    Instant timestamp = Instant.ofEpochSecond(point.getTimestamp().getSeconds(), point.getTimestamp().getNanos());
                    labels.put(TIMESTAMP_COLUMN, MetricsUtils.toInstant(point.getTimestamp()).toEpochMilli());

                    HashMap<String, Object> object = new HashMap<>();
                    object.putAll(labels);
                    if (ts.hasStartTimestamp() && ts.getStartTimestamp() != null) {
                        object.put("startTimestamp", MetricsUtils.toInstant(ts.getStartTimestamp()).toEpochMilli());
                    }
                    switch (point.getValueCase()) {
                        case DOUBLE_VALUE:
                            object.put(NAME, metric.getMetricDescriptor().getName());
                            object.put("doubleValue", point.getDoubleValue());
                            result.add(object);
                            break;
                        case INT64_VALUE:
                            object.put("int64Value", point.getInt64Value());
                            object.put(NAME, metric.getMetricDescriptor().getName());
                            result.add(object);
                            break;
                        case SUMMARY_VALUE:
                            object.put(NAME, metric.getMetricDescriptor().getName());
                            object.put("count", point.getSummaryValue().getCount().getValue());
                            object.put("sum", point.getSummaryValue().getSnapshot().getSum().getValue());
                            for (SummaryValue.Snapshot.ValueAtPercentile p : point.getSummaryValue().getSnapshot().getPercentileValuesList()) {
                                object.put("" + p.getPercentile(), point.getSummaryValue().getSnapshot().getSum().getValue());
                            }
                            result.add(object);
                            break;
                        case DISTRIBUTION_VALUE:
                            object.put(NAME, metric.getMetricDescriptor().getName());
                            object.put("count", point.getDistributionValue().getCount());
                            object.put("sum", point.getDistributionValue().getSum());
                            object.put("variance", point.getDistributionValue().getSumOfSquaredDeviation());
                            for (DistributionValue.Bucket b : point.getDistributionValue().getBucketsList()) {
                                // TODO : Finish this.
                                //dist.put("" + b.getExemplar()., point.);
                            }
                            result.add(object);
                            break;
                    }
                }
            }

            // FIXME : This works for now because we send one metric / timeseries and a single timeseries per metric.
            // logstash supports splitting a list of records so we should use that instead.
            if (result.size() > 0) {
                return this.mapper.writeValueAsString(result.get(0));
            } else {
                throw new RuntimeException("empty serialized object.");
            }

        } catch (SerializationException e) {
            throw e;
        } catch (Exception e) {
            String errMsg = "Error deserializing protobuf message";
            throw new SerializationException(errMsg, e);
        }
    }


    @Override
    public String deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

}
