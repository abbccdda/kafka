package io.confluent.telemetry.events.exporter.http;

import com.google.common.collect.Sets;
import com.google.protobuf.MessageLite;
import io.confluent.telemetry.client.BufferingAsyncTelemetryHttpClient;
import io.confluent.telemetry.client.BufferingAsyncTelemetryHttpClientBatchResult;
import io.confluent.telemetry.events.exporter.Exporter;
import io.confluent.telemetry.events.exporter.ExporterConfig;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class HttpExporter<Data, Req extends MessageLite, Resp extends MessageLite>
        implements Exporter<Data> {

  private static final Logger log = LoggerFactory.getLogger(HttpExporter.class);
  protected Function<Collection<Data>, Req> requestConverter;
  protected Function<ByteBuffer, Resp> responseDeserializer;

  protected BufferingAsyncTelemetryHttpClient<Data, Req, Resp> bufferingClient;
  public volatile boolean canEmitTelemetry = false;
  protected String endpoint;
  private HttpExporterConfig config;


  @Override
  public void configure(Map<String, ?> configs) {
    config = new HttpExporterConfig(configs);
    this.bufferingClient = config
            .<Data, Req, Resp>getBufferingAsyncClientBuilder()
            .setClient(
                    config.<Resp>getClientBuilder()
                            .setResponseDeserializer(responseDeserializer)
                            .setEndpoint(endpoint)
                            .<Req>build()
            )
            .setCreateRequestFn(requestConverter)
            .build();
    // subscribe to batch results.
    this.bufferingClient.getBatchResults().doOnNext(this::trackResponses);
    this.canEmitTelemetry = config.canEmitTelemetry();
  }

  private void trackResponses(
          BufferingAsyncTelemetryHttpClientBatchResult<Data, Resp> batchResult) {
    if (!batchResult.isSuccess()) {
      log.error("Confluent Telemetry Failure", batchResult.getThrowable());
    }
  }

  @Override
  public void emit(Data data) throws RuntimeException {
    if (!canEmitTelemetry) {
      return;
    }
    this.bufferingClient.submit(Collections.singleton(data));
  }

  @Override
  public void close() {
    this.bufferingClient.close();
  }


  @Override
  public void reconfigure(Map<String, ?> configs) {

    HttpExporterConfig config = new HttpExporterConfig(configs);
    String apiKey = config.getString(HttpExporterConfig.API_KEY);
    String apiSecretKey = config.getString(HttpExporterConfig.API_SECRET);
    if (config.canEmitTelemetry()) {
      canEmitTelemetry = true;
    } else {
      canEmitTelemetry = false;
    }
    this.bufferingClient.updateCredentials(apiKey, apiSecretKey);

    Optional.ofNullable(config.getString(HttpExporterConfig.PROXY_URL)).map(URI::create)
            .ifPresent(this.bufferingClient::updateProxyUrl);
    String username = config.getString(HttpExporterConfig.PROXY_USERNAME);
    String password = config.getString(HttpExporterConfig.PROXY_PASSWORD);
    this.bufferingClient.updateProxyCredentials(username, password);
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Sets.union(ExporterConfig.RECONFIGURABLES, HttpExporterConfig.RECONFIGURABLE_CONFIGS);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

  }

  // For testing.
  public void bufferingClient(
          BufferingAsyncTelemetryHttpClient<Data, Req, Resp> bufferingClient) {
    this.bufferingClient = bufferingClient;
  }

}
