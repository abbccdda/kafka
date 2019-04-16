// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.provider;

public class HttpBearerCredentialProvider implements HttpCredentialProvider {
  private final String token;

  public HttpBearerCredentialProvider(String token) {
    this.token = token;
  }

  @Override
  public String getScheme() {
    return "Bearer";
  }

  @Override
  public String getCredentials() {
    return this.token;
  }
}
