// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.provider;

import java.util.Map;

public class HttpBearerCredentialProvider implements HttpCredentialProvider {
  private String token;

  public HttpBearerCredentialProvider() {

  }

  public HttpBearerCredentialProvider(String token) {
    this.token = token;
  }

  @Override
  public void configure(Map<String, ?> configs) {

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
