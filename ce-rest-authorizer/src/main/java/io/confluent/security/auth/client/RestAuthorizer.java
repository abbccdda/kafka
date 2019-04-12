// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.security.auth.client.rest.RestRequest;
import io.confluent.security.auth.client.rest.entities.AuthorizeRequest;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Authorizer;
import io.confluent.security.auth.client.rest.RestClient;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is the implementation of {@link Authorizer} which connects to the given metadata service Urls
 * to perform the operations.
 * <p>
 * An instance of RestAuthorizer can be instantiated by passing configuration properties like below.
 * <pre>
 *     Map<String, Object> configs = new HashMap<>();
 *     configs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, "http://localhost:8080");
 *     Authorizer RestAuthorizer = new RestAuthorizer();
 *     rbacRestAuthorizer.configure(configs);
 * </pre>
 * <p>
 * There are different options available as mentioned in {@link RestClientConfig} like
 * <pre>
 * - {@link RestClientConfig#BOOTSTRAP_METADATA_SERVER_URLS_PROP}.
 * - {@link RestClientConfig#METADATA_SERVER_URL_MAX_AGE_PROP}.
 * - {@link RestClientConfig#BASIC_AUTH_CREDENTIALS_PROVIDER_PROP}.
 * - {@link RestClientConfig#BASIC_AUTH_USER_INFO_PROP}.
 * </pre>
 * <pre>
 * This can be used to authorize list of {@link Action} for a given userPrincipal
 * </pre>
 */
public class RestAuthorizer implements Authorizer {

  private static final Logger log = LoggerFactory.getLogger(RestAuthorizer.class);

  private static final String
          AUTHORIZE_ENDPOINT = "/authorize";

  private static final TypeReference<List<String>>
          AUTHORIZE_RESPONSE_TYPE = new TypeReference<List<String>>() { };

  private RestClient restClient;

  public RestAuthorizer() {};

  public RestAuthorizer(RestClient restClient) {
    this.restClient = restClient;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    if (this.restClient != null) {
      log.warn("Using the existing RestClient instance");
      return;
    }

    this.restClient = new RestClient(configs);
  }

  @Override
  public List<AuthorizeResult> authorize(final KafkaPrincipal sessionPrincipal,
                                         final String host, final List<Action> actions) {

    if (restClient == null)
      throw new IllegalStateException("RestClient has not been initialized.");


    RestRequest request = this.restClient.newRequest(AUTHORIZE_ENDPOINT);

    AuthorizeRequest authorizeRequest =
            new AuthorizeRequest(sessionPrincipal.toString(), host, actions);

    request.setRequest(authorizeRequest);
    request.setResponse(AUTHORIZE_RESPONSE_TYPE);

    try {
      List<String> results = restClient.sendRequest(request);

      return results.stream()
                    .map(AuthorizeResult::valueOf)
                    .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Error occurred" +
              " while executing authorize operation", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (restClient != null)
      restClient.close();
  }

}
