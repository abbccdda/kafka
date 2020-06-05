package io.confluent.telemetry.reporter;

import static io.confluent.telemetry.provider.Utils.buildResourceFromLabels;

import io.confluent.telemetry.provider.Provider;
import io.opencensus.proto.resource.v1.Resource;
import java.util.Map;
import org.apache.kafka.common.metrics.MetricsContext;

public class MockProvider implements Provider {

    public static final String NAMESPACE = "mock";
    private Resource resource;

    @Override
    public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
        return true;
    }

    @Override
    public String domain() {
        return "foo";
    }

    @Override
    public Resource resource() {
        return resource;
    }

    @Override
    public void contextChange(MetricsContext metricsContext) {
        this.resource = buildResourceFromLabels(metricsContext).build();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
