// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client;

import io.confluent.security.auth.client.provider.BuiltInAuthProviders;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders.BasicAuthCredentialProviders;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class RestClientConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String BOOTSTRAP_METADATA_SERVER_URLS_PROP = "bootstrap.metadata.server.urls";
  private static final String BOOTSTRAP_METADATA_SERVER_URLS_DOC = "Comma separated list of bootstrap metadata servers urls to" +
          " which this client connects to. For ex: http://localhost:8080,http://localhost:8081";

  public static final String ENABLE_METADATA_SERVER_URL_REFRESH = "enable.metadata.server.urls.refresh";
  public static final String ENABLE_METADATA_SERVER_URL_REFRESH_DOC = "enables periodic refresh of metadata server urls.";

  public static final String METADATA_SERVER_URL_MAX_AGE_PROP = "metadata.server.urls.max.age.ms";
  public static final Long METADATA_SERVER_URL_MAX_AGE_DEFAULT = 10 * 60 * 1000L;
  public static final String METADATA_SERVER_URL_MAX_AGE_DOC = "The period of time in milliseconds after which we " +
          "force a refresh of metadata server urls.";

  public static final String HTTP_AUTH_CREDENTIALS_PROVIDER_PROP = "http.auth.credentials.provider";
  private static final String HTTP_AUTH_CREDENTIALS_PROVIDER_DEFAULT = "";
  private static final String HTTP_AUTH_CREDENTIALS_PROVIDER_DOC = "HTTP authentication scheme. "
          + "Supported schemes are " + BuiltInAuthProviders.builtInHttpCredentialProviders();

  public static final String BASIC_AUTH_CREDENTIALS_PROVIDER_PROP = "basic.auth.credentials.provider";
  private static final String BASIC_AUTH_CREDENTIALS_PROVIDER_DEFAULT =
          BasicAuthCredentialProviders.USER_INFO.name();
  private static final String BASIC_AUTH_CREDENTIALS_PROVIDER_PROP_DOC =
          "User credentials provider for the HTTP"
                  + " basic authentication. Supported providers are "
                  + BuiltInAuthProviders.builtInBasicAuthCredentialProviders();

  public static final String BASIC_AUTH_USER_INFO_PROP = "basic.auth.user.info";
  private static final String BASIC_AUTH_USER_INFO_PROP_DOC = "Basic user credentials info in the format user:password." +
          " This is required for " + BasicAuthCredentialProviders.USER_INFO.name() + " provider.";

  public static final String TOKEN_AUTH_CREDENTIAL_PROP = "token.auth.credential";
  private static final String TOKEN_AUTH_CREDENTIAL_DEFAULT = "";
  private static final String TOKEN_AUTH_CREDENTIAL_DOC =
          "String representation of an Authentication Token";

  public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
  public static final String REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum amount of time the client will wait "
          + "for the response of a each authorizer request.";

  public static final String HTTP_REQUEST_TIMEOUT_MS_CONFIG = "http.request.timeout.ms";
  public static final String HTTP_REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum amount of time the client will wait "
      + "for the response of a http request. If the response is not received before the timeout "
      + "elapses the client will resend the request if necessary or fail the request if "
      + "all urls are exhausted. This value should less than or equal to " + REQUEST_TIMEOUT_MS_CONFIG + " config";

  static {
    CONFIG = new ConfigDef()
            .define(BOOTSTRAP_METADATA_SERVER_URLS_PROP, Type.LIST, "", Importance.HIGH,
                    BOOTSTRAP_METADATA_SERVER_URLS_DOC)
            .define(HTTP_AUTH_CREDENTIALS_PROVIDER_PROP, Type.STRING, HTTP_AUTH_CREDENTIALS_PROVIDER_DEFAULT,
                    Importance.HIGH, HTTP_AUTH_CREDENTIALS_PROVIDER_DOC)
            .define(BASIC_AUTH_CREDENTIALS_PROVIDER_PROP, Type.STRING, BASIC_AUTH_CREDENTIALS_PROVIDER_DEFAULT,
                    Importance.HIGH,
                    BASIC_AUTH_CREDENTIALS_PROVIDER_PROP_DOC)
            .define(BASIC_AUTH_USER_INFO_PROP, Type.STRING, "", Importance.MEDIUM, BASIC_AUTH_USER_INFO_PROP_DOC)
            .define(ENABLE_METADATA_SERVER_URL_REFRESH, Type.BOOLEAN, true, Importance.LOW,
                ENABLE_METADATA_SERVER_URL_REFRESH_DOC)
            .define(METADATA_SERVER_URL_MAX_AGE_PROP, Type.LONG, METADATA_SERVER_URL_MAX_AGE_DEFAULT, atLeast(0),
                    Importance.LOW, METADATA_SERVER_URL_MAX_AGE_DOC)
            .define(REQUEST_TIMEOUT_MS_CONFIG, Type.INT, 30 * 1000, atLeast(0), Importance.MEDIUM,
                    REQUEST_TIMEOUT_MS_DOC)
            .define(HTTP_REQUEST_TIMEOUT_MS_CONFIG, Type.INT, 10 * 1000, atLeast(0), Importance.MEDIUM,
                HTTP_REQUEST_TIMEOUT_MS_DOC)
            .define(TOKEN_AUTH_CREDENTIAL_PROP, Type.STRING,
                    TOKEN_AUTH_CREDENTIAL_DEFAULT,
                    Importance.LOW,
                    TOKEN_AUTH_CREDENTIAL_DOC)
            .withClientSslSupport();
  }

  public RestClientConfig(Map<?, ?> props) {
    super(CONFIG, props);
    validate();
  }

  private void validate() {
    if (getInt(RestClientConfig.HTTP_REQUEST_TIMEOUT_MS_CONFIG) >
        getInt(RestClientConfig.REQUEST_TIMEOUT_MS_CONFIG))
      throw new ConfigException(RestClientConfig.HTTP_REQUEST_TIMEOUT_MS_CONFIG +
          " config value should be less than or equal to " + RestClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
  }

  @Override
  public String toString() {
    return Utils.mkString(values(), "", "", "=", "%n\t");
  }

  public static void main(String[] args) throws Exception {
    try (PrintStream out = args.length == 0 ? System.out
        : new PrintStream(new FileOutputStream(args[0]), false, StandardCharsets.UTF_8.name())) {
      out.println(CONFIG.toHtmlTable());
      if (out != System.out) {
        out.close();
      }
    }
  }
}
