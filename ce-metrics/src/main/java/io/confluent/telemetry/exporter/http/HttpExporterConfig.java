package io.confluent.telemetry.exporter.http;

import com.google.common.base.Strings;
import com.google.common.base.Verify;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClient;
import io.confluent.observability.telemetry.client.CompressionAlgorithm;
import io.confluent.observability.telemetry.client.TelemetryHttpClient;
import io.confluent.observability.telemetry.client.TelemetryHttpClient.Builder;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsRequest;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsResponse;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.opencensus.proto.metrics.v1.Metric;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for the HttpExporter.
 *
 * Note that we maintain the defaults in the `telemetry-client` library, so many of the defaults
 * in this class are null.
 */
public class HttpExporterConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(HttpExporterConfig.class);

    public static final String PREFIX = ConfluentTelemetryConfig.PREFIX_EXPORTER + "http.";
    public static final String PREFIX_BUFFER = PREFIX + "buffer.";
    public static final String PREFIX_CLIENT = PREFIX + "client.";
    public static final String PREFIX_PROXY = PREFIX + "proxy.";

    public static final String API_KEY = PREFIX + "api.key";
    public static final String API_KEY_DOC = "The API key used to authenticate with the Confluent telemetry API";

    public static final String API_SECRET_KEY = PREFIX + "api.key.secret";
    public static final String API_SECRET_KEY_DOC = "The API secret key used to authenticate with the Confluent Telemetry API";


    public static final String BUFFER_MAX_BATCH_DURATION_MS = PREFIX_BUFFER + "batch.duration.max.ms";
    public static final String BUFFER_MAX_BATCH_DURATION_MS_DOC = "The maximum duration (in millis) to buffer items before sending them upstream";

    public static final String BUFFER_MAX_BATCH_SIZE = PREFIX_BUFFER + "batch.items.max";
    public static final String BUFFER_MAX_BATCH_SIZE_DOC = "The maximum number of items to buffer into a batch before sending them upstream";

    public static final String BUFFER_MAX_PENDING_BATCHES = PREFIX_BUFFER + "pending.batches.max";
    public static final String BUFFER_MAX_PENDING_BATCHES_DOC = "The maximum number of pending batches. If more than this number of batches are pending"
        + "(i.e. there is backpressure) then the oldest batches will be dropped.";

    public static final String BUFFER_MAX_INFLIGHT_SUBMISSIONS = PREFIX_BUFFER + "inflight.submissions.max";
    public static final String BUFFER_MAX_INFLIGHT_SUBMISSIONS_DOC = "The maximum number of in-flight calls to the HTTP service";


    public static final String CLIENT_BASE_URL = PREFIX_CLIENT + "base.url";
    public static final String CLIENT_BASE_URL_DOC = "The base URL for the telemetry receiver (i.e. https://host:port)";

    public static final String CLIENT_REQUEST_TIMEOUT_MS = PREFIX_CLIENT + "request.timeout.ms";
    public static final String CLIENT_REQUEST_TIMEOUT_MS_DOCS = "The request timeout in milliseconds";

    public static final String CLIENT_CONNECT_TIMEOUT_MS = PREFIX_CLIENT + "connect.timeout.ms";
    public static final String CLIENT_CONNECT_TIMEOUT_MS_DOC = "The connect timeout in milliseconds";

    public static final String CLIENT_MAX_ATTEMPTS = PREFIX_CLIENT + "attempts.max";
    public static final String CLIENT_MAX_ATTEMPTS_DOC = "The maximum number of delivery attempts";

    public static final String CLIENT_RETRY_DELAY_SEC = PREFIX_CLIENT + "retry.delay.seconds";
    public static final String CLIENT_RETRY_DELAY_SEC_DOC = "The delay, in seconds, between retry attempts";


    public static final String CLIENT_COMPRESSION = PREFIX_CLIENT + "compression";
    public static final String CLIENT_COMPRESSION_DOC = "HTTP Compression algorithm to use. Either gzip, lz4, or zstd.";


    public static final String PROXY_URL = PREFIX_PROXY + "url";
    public static final String PROXY_URL_DOC = "The URL for an explicit (i.e. not transparent) forward HTTP proxy";

    public static final String PROXY_USERNAME = PREFIX_PROXY + "username";
    public static final String PROXY_USERNAME_DOC = "The username credential for the forward HTTP proxy";

    public static final String PROXY_PASSWORD = PREFIX_PROXY + "password";
    public static final String PROXY_PASSWORD_DOC = "The password credential for the forward HTTP proxy";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(
            API_KEY,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            API_KEY_DOC
        ).define(
            API_SECRET_KEY,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            API_SECRET_KEY_DOC
        ).define(
            BUFFER_MAX_BATCH_DURATION_MS,
            ConfigDef.Type.LONG,
            null,
            ConfigDef.Importance.LOW,
            BUFFER_MAX_BATCH_DURATION_MS_DOC
        ).define(
            BUFFER_MAX_BATCH_SIZE,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            BUFFER_MAX_BATCH_SIZE_DOC
        ).define(
            BUFFER_MAX_PENDING_BATCHES,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            BUFFER_MAX_PENDING_BATCHES_DOC
        ).define(
            BUFFER_MAX_INFLIGHT_SUBMISSIONS,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            BUFFER_MAX_INFLIGHT_SUBMISSIONS_DOC
        ).define(
            CLIENT_BASE_URL,
            ConfigDef.Type.STRING,
            null,
            new URIValidator(),
            ConfigDef.Importance.LOW,
            CLIENT_BASE_URL_DOC
        ).define(
            CLIENT_COMPRESSION,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            CLIENT_COMPRESSION_DOC
        ).define(
            CLIENT_REQUEST_TIMEOUT_MS,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            CLIENT_REQUEST_TIMEOUT_MS_DOCS
        ).define(
            CLIENT_CONNECT_TIMEOUT_MS,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            CLIENT_CONNECT_TIMEOUT_MS_DOC
        ).define(
            CLIENT_MAX_ATTEMPTS,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            CLIENT_MAX_ATTEMPTS_DOC
        ).define(
            CLIENT_RETRY_DELAY_SEC,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            CLIENT_RETRY_DELAY_SEC_DOC
        ).define(
            PROXY_URL,
            ConfigDef.Type.STRING,
            null,
            new URIValidator(),
            ConfigDef.Importance.LOW,
            PROXY_URL_DOC
        ).define(
            PROXY_USERNAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            PROXY_USERNAME_DOC
        ).define(
            PROXY_PASSWORD,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            PROXY_PASSWORD_DOC
        );

    public static void main(String[] args) {
        System.out.println(CONFIG.toRst());
    }

    public HttpExporterConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }

    /**
     * Get the compression algorithm. If the algorithm is unknown, this returns null.
     */
    public CompressionAlgorithm getCompressionAlgorithm(String key) {
        String compressionName = getString(key);
        if (compressionName == null) {
            return null;
        }
        try {
            return CompressionAlgorithm.valueOf(compressionName.toUpperCase(Locale.US));
        } catch (IllegalArgumentException e) {
            log.warn("Unsupported compression algorithm specified for Telemetry Metrics Reporter Client: {}", compressionName);
            return null;
        }
    }

    public TelemetryHttpClient.Builder<TelemetryReceiverSubmitMetricsResponse> getClientBuilder() {
        TelemetryHttpClient.Builder<TelemetryReceiverSubmitMetricsResponse> builder = new Builder<>();

        String apiKey = getString(API_KEY);
        String apiSecretKey = getString(API_SECRET_KEY);
        Verify.verify(
            (Strings.isNullOrEmpty(apiKey) && Strings.isNullOrEmpty(apiSecretKey)) ||
            (!Strings.isNullOrEmpty(apiKey) && !Strings.isNullOrEmpty(apiSecretKey)),
            "Must specify both %s and %s", API_KEY, API_SECRET_KEY);
        if (apiKey != null && apiSecretKey != null) {
            builder.setCredentials(apiKey, apiSecretKey);
        }

        Optional.ofNullable(getString(CLIENT_BASE_URL)).map(URI::create).ifPresent(builder::setBaseUrl);
        Optional.ofNullable(getInt(CLIENT_REQUEST_TIMEOUT_MS)).ifPresent(builder::setRequestTimeout);
        Optional.ofNullable(getInt(CLIENT_CONNECT_TIMEOUT_MS)).ifPresent(builder::setConnectTimeout);
        Optional.ofNullable(getInt(CLIENT_MAX_ATTEMPTS)).ifPresent(builder::setMaxAttempts);
        Optional.ofNullable(getInt(CLIENT_RETRY_DELAY_SEC)).ifPresent(builder::setRetryDelay);
        Optional.ofNullable(getCompressionAlgorithm(CLIENT_COMPRESSION)).ifPresent(builder::setCompression);

        Optional.ofNullable(getString(PROXY_URL)).map(URI::create).ifPresent(builder::setProxyUrl);
        String username = getString(PROXY_USERNAME);
        String password = getString(PROXY_PASSWORD);
        if (username != null && password != null) {
            builder.setProxyCredentials(username, password);
        }
        return builder;
    }

    public BufferingAsyncTelemetryHttpClient.Builder<Metric, TelemetryReceiverSubmitMetricsRequest, TelemetryReceiverSubmitMetricsResponse> getBufferingAsyncClientBuilder() {
        BufferingAsyncTelemetryHttpClient.Builder<Metric, TelemetryReceiverSubmitMetricsRequest, TelemetryReceiverSubmitMetricsResponse>  builder = BufferingAsyncTelemetryHttpClient.newBuilder();

        Optional.ofNullable(getLong(BUFFER_MAX_BATCH_DURATION_MS)).map(Duration::ofMillis).ifPresent(builder::setMaxBatchDuration);
        Optional.ofNullable(getInt(BUFFER_MAX_BATCH_SIZE)).ifPresent(builder::setMaxBatchSize);
        Optional.ofNullable(getInt(BUFFER_MAX_PENDING_BATCHES)).ifPresent(builder::setMaxPendingBatches);
        Optional.ofNullable(getInt(BUFFER_MAX_INFLIGHT_SUBMISSIONS)).ifPresent(builder::setMaxInflightSubmissions);

        return builder;
    }

    private static class URIValidator implements Validator {

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return;
            }
            if (!(value instanceof String)) {
                throw new ConfigException("Valid URI expected: " + name);
            }
            try {
                URI.create((String) value);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(
                    String.format("Invalid URI for property: %s (value: %s)", name, value));
            }
        }
    }
}
