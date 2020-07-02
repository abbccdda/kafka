package io.confluent.telemetry.integration;

import com.github.luben.zstd.ZstdInputStream;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.cloudevents.v0.proto.Spec;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.client.TelemetryHttpClient;
import io.confluent.telemetry.events.exporter.http.EventHttpExporter;
import io.confluent.telemetry.events.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.v1.ConfigEvent;
import io.confluent.telemetry.events.v1.EventServiceRequest;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.opencensus.proto.agent.metrics.v1.ExportMetricsServiceRequest;
import io.opencensus.proto.agent.metrics.v1.ExportMetricsServiceResponse;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.resource.v1.Resource;
import junit.framework.TestCase;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.SerializationException;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.confluent.telemetry.collector.KafkaMetricsCollector.KAFKA_METRICS_LIB;
import static io.confluent.telemetry.collector.MetricsCollector.LABEL_LIBRARY;
import static io.confluent.telemetry.collector.MetricsCollector.LIBRARY_NONE;
import static io.confluent.telemetry.collector.YammerMetricsCollector.YAMMER_METRICS;
import static io.confluent.telemetry.integration.TestUtils.createAdminClient;
import static io.confluent.telemetry.integration.TestUtils.getLabelValueFromFirstTimeSeries;
import static io.confluent.telemetry.integration.TestUtils.labelExists;
import static junit.framework.TestCase.assertEquals;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaServerMultipleExportersTest extends TelemetryClusterTestHarness {

    private static final String TEST_EXPORTER_TOPIC = "test";

    private MockHttpCollector mockHttpServer;

    @Override
    @Before
    public void setUp() throws Exception {
        mockHttpServer = new MockHttpCollector();
        mockHttpServer.start();
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        mockHttpServer.stop();
    }

    protected void injectMetricReporterProperties(Properties props, String brokerList) {
        props.setProperty(KafkaConfig.MetricReporterClassesProp(),
                "io.confluent.telemetry.reporter.TelemetryReporter");
        // disable the default exporters
        props.setProperty(ConfluentTelemetryConfig
                .exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
                + ExporterConfig.ENABLED_CONFIG, "true");
        props.setProperty(ConfluentTelemetryConfig
                .exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
                + HttpExporterConfig.CLIENT_BASE_URL, mockHttpServer.baseUrl());
        props.setProperty(ConfluentTelemetryConfig
                .exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
                + HttpExporterConfig.API_KEY, "fakekey");
        props.setProperty(ConfluentTelemetryConfig
                .exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
                + HttpExporterConfig.API_SECRET, "fakesecret");

        props.setProperty(ConfluentTelemetryConfig
                .exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
                + HttpExporterConfig.BUFFER_MAX_BATCH_SIZE, "1");

        String local = ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME;
        props.setProperty(
                ConfluentTelemetryConfig.exporterPrefixForName(local) + ExporterConfig.ENABLED_CONFIG,
                "true");
        // TODO: Figure out why this doesnot work with default bootstrap servers.
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(local)
                + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(local)
                + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

        props.setProperty(
                ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG,
                ExporterConfig.ExporterType.kafka.name());
        props.setProperty(
                ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.ENABLED_CONFIG,
                "true");
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test")
                + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test")
                + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");
        props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test")
                + KafkaExporterConfig.TOPIC_NAME_CONFIG, TEST_EXPORTER_TOPIC);


        props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
        props.setProperty(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, "");
        props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "region", "test");
        props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "pkc", "pkc-bar");
        props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "region", "test");
        props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "pkc", "pkc-bar");
        props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");
        props.setProperty(ConfluentTelemetryConfig.CONFIG_EVENTS_ENABLE_CONFIG, "true");

        // force flush every message so that we can generate some Yammer timer metrics
        props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
    }

    public static class Verifier {

        private final List<String> libs = Arrays.asList(KAFKA_METRICS_LIB, LIBRARY_NONE, YAMMER_METRICS);

        private Map<String, String> resourceLabels;

        boolean kafkaMetricsPresent = false, yammerMetricsPresent = false, cpuVolumeMetricsPresent = false;


        public boolean verifyEveryRecord(Metric m) {
            // Verify labels

            // Check the resource labels are present
            Resource resource = m.getResource();
            TestCase.assertEquals("kafka", resource.getType());

            resourceLabels = resource.getLabelsMap();

            // Check that the labels from the config are present.
            assertEquals(resourceLabels.get("kafka.region"), "test");
            assertEquals(resourceLabels.get("kafka.pkc"), "pkc-bar");

            // Check that we get all kinds of metrics
            if (labelExists(m, LABEL_LIBRARY)) {

                String lib = getLabelValueFromFirstTimeSeries(m, LABEL_LIBRARY);
                assertTrue(libs.contains(lib));

                switch (lib) {
                    case KAFKA_METRICS_LIB:
                        kafkaMetricsPresent = true;
                        break;
                    case YAMMER_METRICS:
                        yammerMetricsPresent = true;
                        break;
                    case LIBRARY_NONE:
                        cpuVolumeMetricsPresent = true;
                        break;
                }
            }
            return true;
        }

        public boolean allConditionsMet() {
            return kafkaMetricsPresent && yammerMetricsPresent && cpuVolumeMetricsPresent;
        }

        public void runFinalAssertions() {
            assertTrue(kafkaMetricsPresent);
            assertTrue(yammerMetricsPresent);
            assertTrue(cpuVolumeMetricsPresent);
        }
    }

    public static class EventVerifier {


        boolean staticConfigEventPresent = false, dynamicConfigEventPresent = false;

        public boolean verifyEveryRecord(Spec.CloudEvent ce) {
            // Verify labels

            // Check the resource labels are present
            ConfigEvent m = null;
            try {
                m = ConfigEvent.parseFrom(ce.getBinaryData());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
             Map<String, String> resourceLabels = new HashMap<>();

            m.getResource().getAttributesList().forEach(a -> resourceLabels.put(a.getKey(), a.getValue().getStringValue()));

            // Check that the labels from the config are present.
            assertNotNull(resourceLabels.get("kafka.version"));
            assertNotNull(resourceLabels.get("kafka.broker.id"));

            assertEquals("kafka", ce.getSource());

            assertEquals("telemetry-reporter", ce.getAttributesMap().get("subject").getCeString());
            assertEquals(Protobuf.APPLICATION_PROTOBUF, ce.getAttributesMap().get("datacontenttype").getCeString());

            assertNotNull(ce.getAttributesMap().get("time").getCeTimestamp());
            assertNotNull(ce.getId());

            String type = ce.getType();
            assertNotNull(type);

            if (type.endsWith("/config/dynamic")) {
                dynamicConfigEventPresent = true;
            }

            if (type.endsWith("/config/static")) {
                staticConfigEventPresent = true;
            }


            return true;
        }

        public boolean allConditionsMet() {
            return staticConfigEventPresent && dynamicConfigEventPresent;
        }

        public void runFinalAssertions() {
            assertTrue(staticConfigEventPresent);
            assertTrue(dynamicConfigEventPresent);
        }
    }

    public void testMetricsKafkaReporter(Collection<String> topic) {
        long startMs = System.currentTimeMillis();

        Verifier v = new Verifier();
        this.consumer.subscribe(topic);

        while (System.currentTimeMillis() - startMs < 20000 && !v.allConditionsMet()) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                // Verify that the message de-serializes successfully
                Metric m = null;
                try {
                    m = this.serde.deserializer().deserialize(record.topic(), record.headers(), record.value());
                } catch (SerializationException e) {
                    fail("failed to deserialize message " + e.getMessage());
                }

                assertTrue(v.verifyEveryRecord(m));
                if (v.allConditionsMet()) {
                    break;
                }
            }
        }
        v.runFinalAssertions();
    }

    @Test
    public void testMetricsReporters() throws InterruptedException, ExecutionException {
        triggerDynamicConfigEvent();
        testMetricsKafkaReporter(Collections.singleton(KafkaExporterConfig.DEFAULT_TOPIC_NAME));
        testMetricsKafkaReporter(Collections.singleton(TEST_EXPORTER_TOPIC));
        testMetricsHttpExporter();
        testEvents();
    }

    private void testEvents() throws InterruptedException {
        waitForCondition(() -> mockHttpServer.eventVerifier().allConditionsMet(), 30000, "check if all events are emitted");
        mockHttpServer.eventVerifier().runFinalAssertions();
    }

    private void triggerDynamicConfigEvent() throws InterruptedException, ExecutionException {
        // Trigger reconfig event
        ConfigResource broker = new ConfigResource(Type.BROKER, "");
        AdminClient adminClient = createAdminClient(brokerList);
        String prefix = ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME);
        ConfigEntry entryKey = new ConfigEntry(prefix + HttpExporterConfig.API_KEY, "shiny2newfakekey");
        ConfigEntry entrySecret = new ConfigEntry(prefix + HttpExporterConfig.API_SECRET, "shiny2newfakesecret");
        Collection<AlterConfigOp> configs = ImmutableSet.of(new AlterConfigOp(entryKey, OpType.SET), new AlterConfigOp(entrySecret, OpType.SET));
        adminClient.incrementalAlterConfigs(Collections.singletonMap(broker, configs)).all().get();
    }

    private void testMetricsHttpExporter() throws InterruptedException {
        waitForCondition(() -> mockHttpServer.metricsVerifier().allConditionsMet(), "check if http endpoint had received expected events");
        mockHttpServer.metricsVerifier().runFinalAssertions();
    }

    public static class MockHttpCollector {

        private  Verifier v = new Verifier();
        private EventVerifier eventVerifier = new EventVerifier();
        private final int port = 0;
        private Server server;

        public Verifier metricsVerifier() {
            return v;
        }

        public EventVerifier eventVerifier() {
            return eventVerifier;
        }

        public String baseUrl() {
            return "http://localhost:" + port();
        }

        public void start() throws Exception {
            server = new Server(port);

            ContextHandler context = new ContextHandler("/v1");
            context.setHandler(new DefaultHandler());

            ContextHandler contextFR = new ContextHandler(EventHttpExporter.V1_EVENTS_ENDPOINT);
            contextFR.setAllowNullPathInfo(true);
            contextFR.setHandler(new EventHandler(eventVerifier));

            ContextHandler contextIT = new ContextHandler(TelemetryHttpClient.V1_METRICS_ENDPOINT);
            contextIT.setAllowNullPathInfo(true);
            contextIT.setHandler(new MetricsHandler(this.v));


            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(new Handler[] {contextFR, contextIT});

            server.setHandler(contexts);

            // Access logs for debugging.
            Slf4jRequestLogWriter slf4jRequestLogWriter = new Slf4jRequestLogWriter();
            slf4jRequestLogWriter.setLoggerName("test.requestlog");
            String requestLogFormat = CustomRequestLog.EXTENDED_NCSA_FORMAT;
            CustomRequestLog requestLog = new CustomRequestLog(slf4jRequestLogWriter, requestLogFormat);
            server.setRequestLog(requestLog);

            server.start();
        }

        public void stop() throws Exception {
            server.stop();
        }

        public int port() {
            return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        }

    }

    public static class MetricsHandler extends AbstractHandler {

        private static final Logger log = LoggerFactory.getLogger(MetricsHandler.class);

        public MetricsHandler(Verifier v) {
            this.v = v;
        }

        private Verifier v;

        public void handle(String s, Request request, HttpServletRequest httpServletRequest,
                           HttpServletResponse httpServletResponse) throws IOException {

            try {
                ExportMetricsServiceRequest r = ExportMetricsServiceRequest.parseFrom(new ZstdInputStream(request.getInputStream()));
                r.getMetricsList().forEach(m -> {
                    assertTrue(v.verifyEveryRecord(m));
                });
            } catch (Throwable e) {
                log.error("error deserializing request", e);
            }

            request.setHandled(true);
            httpServletResponse.addHeader("Content-Type", "application/x-protobuf");
            ExportMetricsServiceResponse.newBuilder().build().writeTo(httpServletResponse.getOutputStream());
        }
    }

    public static class EventHandler extends AbstractHandler {
        private static final Logger log = LoggerFactory.getLogger(MetricsHandler.class);

        public EventHandler(EventVerifier verifier) {
            this.verifier = verifier;
        }

        EventVerifier verifier;

        public void handle(String s, Request request, HttpServletRequest httpServletRequest,
                           HttpServletResponse httpServletResponse) throws IOException {
            try {
            EventServiceRequest r = EventServiceRequest.parseFrom(new ZstdInputStream(request.getInputStream()));

            r.getEventsList().forEach(m -> {
                try {
                    log.trace(JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace().print(m));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }

                assertTrue(verifier.verifyEveryRecord(m));

            });
            } catch (Throwable e) {
                log.error("error deserializing request", e);
            }

            request.setHandled(true);
            httpServletResponse.addHeader("Content-Type", "application/x-protobuf");
            ExportMetricsServiceResponse.newBuilder().build().writeTo(httpServletResponse.getOutputStream());
        }
    }

}
