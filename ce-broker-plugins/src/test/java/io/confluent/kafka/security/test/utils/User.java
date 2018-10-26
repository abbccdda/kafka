// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.test.utils;

import io.confluent.kafka.test.utils.SecurityTestUtils;
import java.io.File;

public class User {
  public final String name;
  public final String fullPrincipal;
  public final String jaasConfig;

  public User(String name, String fullPrincipal, String jaasConfig) {
    this.name = name;
    this.fullPrincipal = fullPrincipal;
    this.jaasConfig = jaasConfig;
  }

  public static User scramUser(String name, String scramSecret) {
    String jaasConfig = SecurityTestUtils.scramSaslJaasConfig(name, scramSecret);
    return new User(name, name, jaasConfig);
  }

  public static User gssapiUser(String name, String fullPrincipal, File keytabFile, String serviceName) {
    String jaasConfig = SecurityTestUtils.gssapiSaslJaasConfig(keytabFile, fullPrincipal, serviceName);
    return new User(name, fullPrincipal, jaasConfig);
  }
}
