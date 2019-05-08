// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.provider;

import io.confluent.security.auth.client.RestClientConfig;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class HttpBasicCredentialProvider implements HttpCredentialProvider {
  private String userInfo;

  public HttpBasicCredentialProvider() { }

  public HttpBasicCredentialProvider(String userInfo) {
    this.userInfo = encodeUserInfo(userInfo);
  }

  private String encodeUserInfo(String userInfo) {
    return Base64.getEncoder().encodeToString(userInfo.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (this.userInfo != null) {
      throw new IllegalStateException("HttpBasicCredentialProvider already initialized");
    }

    RestClientConfig rbacClientConfig = new RestClientConfig(configs);
    String basicAuthProvider =
            rbacClientConfig.getString(RestClientConfig.BASIC_AUTH_CREDENTIALS_PROVIDER_PROP);

    BasicAuthCredentialProvider basicAuthCredentialProvider =
            BuiltInAuthProviders.loadBasicAuthCredentialProvider(basicAuthProvider);

    basicAuthCredentialProvider.configure(configs);
    this.userInfo = encodeUserInfo(basicAuthCredentialProvider.getUserInfo());
  }

  @Override
  public String getScheme() {
    return "Basic";
  }

  @Override
  public String getCredentials() {
    return this.userInfo;
  }
}