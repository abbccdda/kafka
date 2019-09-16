// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.integration.cluster;

import io.confluent.kafka.multitenant.MultiTenantInterceptor;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.MultiTenantPrincipalBuilder;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kafka.security.authorizer.AclAuthorizer$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * Encapsulation of physical cluster consisting of multiple logical
 * clusters. A physical cluster is shared across multiple tenants and
 * each tenant is allocated a logical cluster. SASL/SCRAM is used as
 * the authentication protocol in tests to easily add or remove tenants
 * and users.
 * <p>
 * <b>User Model</b>:
 * <ul>
 *   <li>Tenant: used for resource isolation, associated with a unique prefix</li>
 *   <li>Logical cluster: uniquely identifies tenant (and tenant prefix)</li>
 *   <li>User id: used in ACLs, identifies tenant, each tenant may have multiple users</li>
 *   <li>User name: Used during authentication, uniquely identifies user and logical cluster</li>
 *   <li>APIKey: Included in username during authentication, identifies user within logical cluster</li>
 *   <li>Super users: Tenant super users have access to all resources of the tenant</li>
 * </ul>
 * </p>
 */
public class PhysicalCluster {

  public static final int DEFAULT_NUMBER_OF_BROKERS = 1;
  public static final KafkaPrincipal BROKER_PRINCIPAL =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "broker");
  private static final Pattern SASL_USERNAME_PATTERN =
      Pattern.compile("(?<clusterId>[^_]*)_(?<apiKey>.*)");
  // We only create one physical cluster in a test, so it is safe to use a static instance.
  private static PhysicalCluster instance;

  private final Properties overrideProps;
  private final EmbeddedKafkaCluster kafkaCluster;
  private AdminClient superAdminClient;
  private final Random random;
  private final Map<Integer, UserMetadata> usersById;
  private final Map<String, UserMetadata> usersByApiKey;
  private final Map<String, LogicalCluster> logicalClusters;
  private final int numberOfBrokers;

  public PhysicalCluster(int brokers, Properties props) {
    kafkaCluster = new EmbeddedKafkaCluster();
    random = new Random();
    usersById = new HashMap<>();
    usersByApiKey = new ConcurrentHashMap<>();
    logicalClusters = new HashMap<>();
    numberOfBrokers = brokers;

    this.overrideProps = new Properties();
    this.overrideProps.putAll(props);
    this.overrideProps.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
        MultiTenantScramPrincipalBuilder.class.getName());
    this.overrideProps.put("listener.name.external.broker.interceptor.class",
        MultiTenantInterceptor.class.getName());
  }

  public synchronized void start() throws Exception {
    instance = this;
    kafkaCluster.startZooKeeper();

    overrideProps.setProperty(AclAuthorizer$.MODULE$.SuperUsersProp(),
        BROKER_PRINCIPAL.toString());
    kafkaCluster.startBrokers(numberOfBrokers, KafkaTestUtils.brokerConfig(overrideProps));
  }

  public synchronized void shutdown() {
    try {
      if (superAdminClient != null) {
        superAdminClient.close();
      }
      kafkaCluster.shutdown();
    } finally {
      instance = null;
    }
  }

  public String bootstrapServers() {
    return kafkaCluster.bootstrapServers();
  }

  public EmbeddedKafkaCluster kafkaCluster() {
    return kafkaCluster;
  }

  public synchronized LogicalCluster createLogicalCluster(String clusterId, int adminUserId,
      Integer... serviceIds) throws Exception {
    if (logicalClusters.containsKey(clusterId)) {
      throw new IllegalArgumentException("Logical cluster " + clusterId + " already exists");
    }
    UserMetadata adminUser = getOrCreateUser(adminUserId, true);
    LogicalCluster logicalCluster = new LogicalCluster(this, clusterId, adminUser);
    logicalClusters.put(clusterId, logicalCluster);
    for (Integer userId : serviceIds) {
      logicalCluster.addUser(getOrCreateUser(userId, false));
    }
    return logicalCluster;
  }

  private synchronized UserMetadata getOrCreateUser(int userId, boolean isSuperUser) {
    UserMetadata userMetadata = usersById.get(userId);
    if (userMetadata != null) {
      return userMetadata;
    }
    String apiKey = "APIKEY" + userId;
    String apiSecret = "APISECRET-" + random.nextLong();
    userMetadata = new UserMetadata(userId, apiKey, apiSecret, isSuperUser);
    usersById.put(userId, userMetadata);
    usersByApiKey.put(apiKey, userMetadata);
    return userMetadata;
  }

  public MultiTenantPrincipal principal(String saslUserName) {
    Matcher matcher = SASL_USERNAME_PATTERN.matcher(saslUserName);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid SASL user name " + saslUserName);
    }
    String apiKey = matcher.group("apiKey");
    UserMetadata user = usersByApiKey.get(apiKey);
    if (user == null) {
      throw new IllegalArgumentException("APIKey not found " + apiKey);
    }
    String logicalClusterId = matcher.group("clusterId");
    TenantMetadata tenantMetadata = new TenantMetadata.Builder(logicalClusterId)
        .superUser(user.isSuperUser())
        .build();
    return new MultiTenantPrincipal(String.valueOf(user.userId()), tenantMetadata);
  }

  public synchronized AdminClient superAdminClient() {
    if (superAdminClient == null) {
      superAdminClient = KafkaTestUtils.createAdminClient(
          kafkaCluster.bootstrapServers("INTERNAL"),
          SecurityProtocol.PLAINTEXT,
          "",
          "");
    }
    return superAdminClient;
  }

  public static class MultiTenantScramPrincipalBuilder extends MultiTenantPrincipalBuilder {
    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
      if (context.securityProtocol() == SecurityProtocol.SASL_PLAINTEXT) {
        SaslAuthenticationContext saslContext = (SaslAuthenticationContext) context;
        String authzId = saslContext.server().getAuthorizationID();
        return instance.principal(authzId);
      } else if (context.securityProtocol() == SecurityProtocol.PLAINTEXT) {
        return BROKER_PRINCIPAL;
      } else {
        return super.build(context);
      }
    }
  }
}
