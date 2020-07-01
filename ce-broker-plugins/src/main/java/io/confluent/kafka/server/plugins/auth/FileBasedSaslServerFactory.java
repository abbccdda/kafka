// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import java.util.Optional;
import javax.net.ssl.SNIHostName;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslServer;
import java.util.List;

public class FileBasedSaslServerFactory extends PlainSaslServer.PlainSaslServerFactory {

  public FileBasedSaslServerFactory() {
    super(new SaslServerSupplier() {
      @Override
      public SaslServer get(List<AppConfigurationEntry> jaasContextEntries, Optional<SNIHostName> sniHostNameOptional) {
        return new PlainSaslServer(jaasContextEntries, new FileBasedPlainSaslAuthenticator(), sniHostNameOptional);
      }
    });
  }
}
