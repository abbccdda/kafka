package io.confluent.security.auth.client.provider;

public interface HttpCredentialProvider {
  String getScheme();
  String getCredentials();
}
