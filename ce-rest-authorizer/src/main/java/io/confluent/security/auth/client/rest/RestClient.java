// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.auth.client.provider.BasicAuthCredentialProvider;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders.BasicAuthCredentialProviders;
import io.confluent.security.auth.client.rest.entities.ErrorMessage;
import io.confluent.security.auth.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Rest client for sending RBAC requests to the metadata service.
 */
public class RestClient implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(RestClient.class);

  private static final int HTTP_CONNECT_TIMEOUT_MS = 60000;
  private static final int HTTP_READ_TIMEOUT_MS = 60000;
  private static final int JSON_PARSE_ERROR_CODE = 50005;

  private static final String ACTIVE_NODES_END_POINT = "/activenodes/%s";

  private static final TypeReference<List<String>> ACTIVE_URLS_RESPONSE_TYPE = new TypeReference<List<String>>() {
  };

  private static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;
  private static ObjectMapper jsonDeserializer = new ObjectMapper();
  private final Time time;

  static {
    DEFAULT_REQUEST_PROPERTIES = new HashMap<>();
    DEFAULT_REQUEST_PROPERTIES.put("Content-Type", "application/json");
  }

  private final List<String> bootstrapMetadataServerURLs;
  private final int requestTimeout;
  private final int httpRequestTimeout;
  private volatile List<String> activeMetadataServerURLs;
  private final String protocol;

  private SSLSocketFactory sslSocketFactory;
  private BasicAuthCredentialProvider basicAuthCredentialProvider;
  private ScheduledExecutorService urlRefreshscheduler;
  private RequestSender requestSender = new HTTPRequestSender();

  public RestClient(final Map<String, ?> configs) {
    this(configs, Time.SYSTEM);
  }

  public RestClient(final Map<String, ?> configs, final Time time) {
    this.time = time;
    RestClientConfig rbacClientConfig = new RestClientConfig(configs);
    this.bootstrapMetadataServerURLs = rbacClientConfig.getList(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP);
    if (bootstrapMetadataServerURLs.isEmpty())
      throw new ConfigException("Missing required bootstrap metadata server url list.");

    this.protocol = protocol(bootstrapMetadataServerURLs);
    this.requestTimeout = rbacClientConfig.getInt(RestClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
    this.httpRequestTimeout = rbacClientConfig.getInt(RestClientConfig.HTTP_REQUEST_TIMEOUT_MS_CONFIG);

    //set basic auth provider
    String basicAuthProvider = (String) configs.get(RestClientConfig.BASIC_AUTH_CREDENTIALS_PROVIDER_PROP);
    String basicAuthProviderName = basicAuthProvider == null || basicAuthProvider.isEmpty()
            ? BasicAuthCredentialProviders.NONE.name() : basicAuthProvider;
    basicAuthCredentialProvider = BuiltInAuthProviders.loadBasicAuthCredentialProvider(basicAuthProviderName);
    basicAuthCredentialProvider.configure(configs);

    //set ssl socket factory
    if (rbacClientConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) != null)
      sslSocketFactory = createSslSocketFactory(rbacClientConfig);

    activeMetadataServerURLs = bootstrapMetadataServerURLs;
    if (rbacClientConfig.getBoolean(RestClientConfig.ENABLE_METADATA_SERVER_URL_REFRESH))
      scheduleMetadataServiceUrlRefresh(rbacClientConfig);
  }

  private String protocol(final List<String> bootstrapMetadataServerURLs) {
    try {
      return new URL(bootstrapMetadataServerURLs.get(0)).getProtocol();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Error while fetching URL protocol", e);
    }
  }

  private void scheduleMetadataServiceUrlRefresh(final RestClientConfig rbacClientConfig) {
    //get active metadata server urls
    try {
      activeMetadataServerURLs = getActiveMetadataServerURLs();
      if (activeMetadataServerURLs.isEmpty())
        throw new ConfigException("Active metadata server url list is empty.");
    } catch (Exception e) {
      throw new RuntimeException("Error while fetching activeMetadataServerURLs.", e);
    }

    //periodic refresh of metadata server urls
    Long metadataServerUrlsMaxAgeMS = rbacClientConfig.getLong(RestClientConfig.METADATA_SERVER_URL_MAX_AGE_PROP);
    urlRefreshscheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setDaemon(true);
      return t;
    });

    class MetadataServerUrlFetcher implements Runnable {
      @Override
      public void run() {
        try {
          activeMetadataServerURLs = getActiveMetadataServerURLs();
        } catch (Exception e) {
          log.error("Error while refreshing active metadata server urls, retrying", e);
          urlRefreshscheduler.schedule(new MetadataServerUrlFetcher(), 100, TimeUnit.MILLISECONDS);
        }
      }
    }

    urlRefreshscheduler.scheduleAtFixedRate(new MetadataServerUrlFetcher(), metadataServerUrlsMaxAgeMS,
            metadataServerUrlsMaxAgeMS, TimeUnit.MILLISECONDS);
  }

  private SSLSocketFactory createSslSocketFactory(final RestClientConfig rbacClientConfig) {
    SslFactory sslFactory = new SslFactory(Mode.CLIENT);
    sslFactory.configure(rbacClientConfig.values());
    return sslFactory.sslContext().getSocketFactory();
  }

  private List<String> getActiveMetadataServerURLs()
          throws IOException, RestClientException, URISyntaxException {

    RestRequest request = this.newRequest(String.format(ACTIVE_NODES_END_POINT, protocol));
    request.setResponse(ACTIVE_URLS_RESPONSE_TYPE);

    return this.sendRequest(request);
  }

  private String buildRequestUrl(String baseUrl, String path) {
    // Join base URL and path, collapsing any duplicate forward slash delimiters
    return baseUrl.replaceFirst("/$", "") + "/" + path.replaceFirst("^/", "");
  }

  private void setupSsl(HttpURLConnection connection) {
    if (connection instanceof HttpsURLConnection && sslSocketFactory != null) {
      ((HttpsURLConnection) connection).setSSLSocketFactory(sslSocketFactory);
    }
  }

  private void setBasicAuthRequestHeader(HttpURLConnection connection) {
    String userInfo;
    if (basicAuthCredentialProvider != null
            && (userInfo = basicAuthCredentialProvider.getUserInfo()) != null) {
      String authHeader = Base64.getEncoder().encodeToString(userInfo.getBytes(StandardCharsets.UTF_8));
      connection.setRequestProperty("Authorization", "Basic " + authHeader);
    }
  }

  void requestSender(RequestSender requestSender) {
    this.requestSender = requestSender;
  }

  public RestRequest newRequest(String path) {
    return new RestRequest(this.protocol, path);
  }

  public <T> T sendRequest(RestRequest request)
          throws IOException, RestClientException, URISyntaxException {
    long begin = time.milliseconds();
    long remainingWaitMs = requestTimeout;
    long elapsed;

    UrlSelector urlSelector = new UrlSelector(activeMetadataServerURLs);
    for (int i = 0, n = urlSelector.size(); i < n; i++) {
      try {
        URI mds = new URI(urlSelector.current());
        request.setHost(mds.getHost());
        request.setPort(mds.getPort());
        return requestSender.send(request,
                remainingWaitMs);
      } catch (IOException e) {
        urlSelector.fail();
        if (i == n - 1) {
          throw e; // Raise the exception since we have no more urls to try
        }
      }

      elapsed = time.milliseconds() - begin;
      if (elapsed >= requestTimeout) {
        throw new TimeoutException("Request aborted due to timeout.");
      }
      remainingWaitMs = requestTimeout - elapsed;
    }
    throw new IOException("Internal HTTP retry error"); // Can't get here
  }

  @Override
  public void close() {
    if (urlRefreshscheduler != null)
      urlRefreshscheduler.shutdownNow();
  }

  private class HTTPRequestSender implements RequestSender {

    ExecutorService executor = new ThreadPoolExecutor(
            0,
            Integer.MAX_VALUE,
            1,
            TimeUnit.MINUTES,
            new SynchronousQueue<>());

    @Override
    public <T> T send(RestRequest request, final long requestTimeout) throws IOException, RestClientException {
      Future<T> f = submit(request);
      try {
        return f.get(Math.min(requestTimeout, httpRequestTimeout), TimeUnit.MILLISECONDS);
      } catch (Throwable e) {
        if (e instanceof ExecutionException) {
          e = e.getCause();
        }
        if (e instanceof RestClientException) {
          throw (RestClientException) e;
        } else if (e instanceof IOException) {
          throw (IOException) e;
        } else {
          throw new RuntimeException(e);
        }
      }
    }

    private <T> Future<T> submit(RestRequest request) {
      return executor.submit(() -> {
        HttpURLConnection connection = null;
        try {
          URL url = request.build().toURL();
          connection = (HttpURLConnection) url.openConnection();

          connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
          connection.setReadTimeout(HTTP_READ_TIMEOUT_MS);

          setupSsl(connection);
          request.configureConnection(connection);
          connection.setUseCaches(false);

          for (Map.Entry<String, String> entry : DEFAULT_REQUEST_PROPERTIES.entrySet()) {
            connection.setRequestProperty(entry.getKey(), entry.getValue());
          }

          if (connection.getDoOutput()) {
            try (OutputStream os = connection.getOutputStream()) {
              request.writeRequestBody(os);
              os.flush();
            } catch (IOException e) {
              log.error("Failed to send HTTP request to endpoint: " + url, e);
              throw e;
            }
          }

          int responseCode = connection.getResponseCode();
          if (responseCode == HttpURLConnection.HTTP_OK) {
            InputStream is = connection.getInputStream();
            T result = request.readResponse(is);
            is.close();
            return result;
          } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
            return null;
          } else {
            InputStream es = connection.getErrorStream();
            ErrorMessage errorMessage;
            try {
              errorMessage = jsonDeserializer.readValue(es, ErrorMessage.class);
            } catch (JsonProcessingException e) {
              errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, e.getMessage());
            }
            es.close();
            throw new RestClientException(errorMessage.message(), responseCode,
                    errorMessage.errorCode());
          }

        } finally {
          if (connection != null) {
            connection.disconnect();
          }
        }
      });
    }
  }

}
