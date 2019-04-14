// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.provider;

import io.confluent.security.auth.client.RestClientConfig;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class HttpBasicCredentialProvider implements HttpCredentialProvider {
  private BasicAuthCredentialProvider basicAuthCredentialProvider;

  public HttpBasicCredentialProvider(Map<String, ?> configs) {
    //set basic auth provider
    String basicAuthProvider = (String) configs.get(RestClientConfig.BASIC_AUTH_CREDENTIALS_PROVIDER_PROP);
    String basicAuthProviderName = basicAuthProvider == null || basicAuthProvider.isEmpty()
            ? BuiltInAuthProviders.BasicAuthCredentialProviders.NONE.name() : basicAuthProvider;
    basicAuthCredentialProvider = BuiltInAuthProviders.loadBasicAuthCredentialProvider(basicAuthProviderName);
    basicAuthCredentialProvider.configure(configs);
  }

  @Override
  public String getScheme() {
    return "Basic";
  }

  @Override
  public String getCredentials() {
    String userInfo = "ANONYMOUS";
    if (basicAuthCredentialProvider != null
            && (userInfo = basicAuthCredentialProvider.getUserInfo()) != null) {
      userInfo = Base64.getEncoder().encodeToString(userInfo.getBytes(StandardCharsets.UTF_8));
    }
    return userInfo;
  }
}