package io.confluent.security.auth.client.provider;

import org.apache.kafka.common.Configurable;

public interface HttpCredentialProvider extends Configurable {
  String getScheme();
  String getCredentials();
}
