// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.authorizer.provider.Provider;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.audit.AuditLogProvider;

public class ConfluentAuthorizerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String ACCESS_RULE_PROVIDERS_PROP = "confluent.authorizer.access.rule.providers";
  private static final String ACCESS_RULE_PROVIDERS_DEFAULT = AccessRuleProviders.ZK_ACL.name();
  private static final String ACCESS_RULE_PROVIDERS_DOC = "List of access rule providers enabled. "
      + " Access rule providers supported are " + ConfluentBuiltInProviders.builtInAccessRuleProviders()
      + ". ACL-based provider is enabled by default.";

  public static final String INIT_TIMEOUT_PROP = "confluent.authorizer.init.timeout.ms";
  private static final int INIT_TIMEOUT_DEFAULT = 600000;
  private static final String INIT_TIMEOUT_DOC = "The number of milliseconds to wait for"
      + " authorizer to start up and initialize any metadata from Kafka topics. On brokers of"
      + " the cluster hosting metadata topics, inter-broker listeners will be started prior"
      + " to initialization of authorizer metadata from Kafka topics.";


  // SimpleAclAuthorizer configs

  public static final String ALLOW_IF_NO_ACLS_PROP = "allow.everyone.if.no.acl.found";
  private static final boolean ALLOW_IF_NO_ACLS_DEFAULT = false;
  private static final String ALLOW_IF_NO_ACLS_DOC =
      "Boolean flag that indicates if everyone is allowed access to a resource if no ACL is found.";

  public static final String SUPER_USERS_PROP = "super.users";
  private static final String SUPER_USERS_DEFAULT = "";
  private static final String SUPER_USERS_DOC = "Semicolon-separated list of principals of"
      + " super users who are allowed access to all resources.";

  public static final String BROKER_USERS_PROP = "broker.users";
  private static final String BROKER_USERS_DEFAULT = "";
  private static final String BROKER_USERS_DOC = "Semicolon-separated list of principals of"
      + " users who are allowed access to all resources on inter broker listener.";

  public static final String MIGRATE_ACLS_FROM_ZK_PROP = "confluent.authorizer.migrate.acls.from.zk";
  private static final boolean MIGRATE_ACLS_FROM_ZK_DEFAULT = false;
  private static final String MIGRATE_ACLS_FROM_ZK_DOC = "This boolean flag is used when we want to migrate ZK ACLs" +
      " to metadata service. For migration, configure both ACL and RBAC providers and do a rolling restart of the cluster." +
      " Also enable this flag on last broker of rolling restart. Based on this flag, last broker will copy the ACLs to" +
      " metadata service. After migration, remove ACL provider and remove this flag from broker and do a rolling restart." +
      " Please check migration docs for more details.";

  public static final String ACL_MIGRATION_BATCH_SIZE_PROP = "confluent.authorizer.acl.migration.batch.size";
  private static final int ACL_MIGRATION_BATCH_SIZE_DEFAULT = 1000;
  private static final String ACL_MIGRATION_BATCH_SIZE_DOC =
      "Batch size used while migrating ACLs from zk to metadata service.";

  static {
    CONFIG = new ConfigDef()
        .define(ALLOW_IF_NO_ACLS_PROP, Type.BOOLEAN, ALLOW_IF_NO_ACLS_DEFAULT,
            Importance.MEDIUM, ALLOW_IF_NO_ACLS_DOC)
        .define(SUPER_USERS_PROP, Type.STRING, SUPER_USERS_DEFAULT,
            Importance.MEDIUM, SUPER_USERS_DOC)
        .define(BROKER_USERS_PROP, Type.STRING, BROKER_USERS_DEFAULT,
            Importance.MEDIUM, BROKER_USERS_DOC)
        .define(ACCESS_RULE_PROVIDERS_PROP, Type.LIST, ACCESS_RULE_PROVIDERS_DEFAULT,
            Importance.MEDIUM, ACCESS_RULE_PROVIDERS_DOC)
        .define(INIT_TIMEOUT_PROP, Type.INT, INIT_TIMEOUT_DEFAULT,
            atLeast(0), Importance.LOW, INIT_TIMEOUT_DOC)
        .define(MIGRATE_ACLS_FROM_ZK_PROP, Type.BOOLEAN, MIGRATE_ACLS_FROM_ZK_DEFAULT,
            Importance.MEDIUM, MIGRATE_ACLS_FROM_ZK_DOC)
        .define(ACL_MIGRATION_BATCH_SIZE_PROP, Type.INT, ACL_MIGRATION_BATCH_SIZE_DEFAULT,
            Importance.MEDIUM, ACL_MIGRATION_BATCH_SIZE_DOC);
  }
  public final boolean allowEveryoneIfNoAcl;
  public final boolean migrateAclsFromZK;
  public Set<KafkaPrincipal> superUsers;
  public Set<KafkaPrincipal> brokerUsers;
  public final Duration initTimeout;

  public ConfluentAuthorizerConfig(Map<?, ?> props) {
    super(CONFIG, props);

    allowEveryoneIfNoAcl = getBoolean(ALLOW_IF_NO_ACLS_PROP);

    if (getList(ACCESS_RULE_PROVIDERS_PROP).isEmpty())
      throw new ConfigException("No access rule providers specified");

    this.superUsers = parseUsers(getString(ConfluentAuthorizerConfig.SUPER_USERS_PROP));
    this.brokerUsers = parseUsers(getString(ConfluentAuthorizerConfig.BROKER_USERS_PROP));
    initTimeout = Duration.ofMillis(getInt(INIT_TIMEOUT_PROP));
    migrateAclsFromZK = getBoolean(MIGRATE_ACLS_FROM_ZK_PROP);
  }

  public static Set<KafkaPrincipal> parseUsers(String su) {
    if (su != null && !su.trim().isEmpty()) {
      String[] users = su.split(";");
      return Arrays.stream(users)
          .map(user -> SecurityUtils.parseKafkaPrincipal(user.trim()))
          .collect(Collectors.toSet());
    } else {
      return Collections.emptySet();
    }
  }

  public final Providers createProviders(String clusterId, final AuditLogProvider auditLogProvider) {
    List<String> authProviderNames = getList(ACCESS_RULE_PROVIDERS_PROP);
    // Multitenant ACLs are included in the MultiTenantProvider, so include only the MultiTenantProvider
    if (authProviderNames.contains(AccessRuleProviders.ZK_ACL.name())
        && authProviderNames.contains(AccessRuleProviders.MULTI_TENANT.name())) {
      authProviderNames = new ArrayList<>(authProviderNames);
      authProviderNames.remove(AccessRuleProviders.ZK_ACL.name());
    }
    if (authProviderNames.isEmpty())
      throw new ConfigException("No access rule providers specified");

    List<AccessRuleProvider> accessRuleProviders =
        ConfluentBuiltInProviders.loadAccessRuleProviders(authProviderNames);
    Set<Provider> providers = new HashSet<>(accessRuleProviders);

    GroupProvider groupProvider = createProvider(GroupProvider.class,
        ConfluentBuiltInProviders::loadGroupProvider,
        providers);
    providers.add(groupProvider);

    MetadataProvider metadataProvider = createProvider(MetadataProvider.class,
        ConfluentBuiltInProviders::loadMetadataProvider,
        providers);
    providers.add(metadataProvider);

    if (clusterId != null) {
      ClusterResource clusterResource = new ClusterResource(clusterId);
      providers.forEach(provider -> {
        if (provider instanceof ClusterResourceListener) {
          ((ClusterResourceListener) provider).onUpdate(clusterResource);
        }
      });
      if (auditLogProvider instanceof ClusterResourceListener) {
        ((ClusterResourceListener) auditLogProvider).onUpdate(clusterResource);
      }
    }
    providers.forEach(provider -> provider.configure(originals()));

    return new Providers(accessRuleProviders, groupProvider, metadataProvider);
  }

  @SuppressWarnings("unchecked")
  private <T extends Provider> T createProvider(Class<T> providerClass,
      Function<Map<String, ?>, T> creator,
      Collection<? extends Provider> otherProviders) {
    Set<String> otherProviderNames = otherProviders.stream().map(Provider::providerName)
        .collect(Collectors.toSet());
    for (Provider provider : otherProviders) {
      if (otherProviderNames.contains(provider.providerName()) && providerClass.isInstance(provider))
        return (T) provider;
    }
    return creator.apply(originals());
  }

  public static Set<String> accessRuleProviders(Map<String, ?> configs) {
    String accessProviders = (String) configs.get(ACCESS_RULE_PROVIDERS_PROP);
    if (accessProviders == null || accessProviders.isEmpty())
      return Collections.emptySet();
    return Utils.mkSet(accessProviders.trim().split("\\s*,\\s*"));
  }

  @Override
  public String toString() {
    return Utils.mkString(values(), "", "", "=", "%n\t");
  }

  public static class Providers {
    public final List<AccessRuleProvider> accessRuleProviders;
    public final GroupProvider groupProvider;
    public final MetadataProvider metadataProvider;

    private Providers(List<AccessRuleProvider> accessRuleProviders,
        GroupProvider groupProvider,
        MetadataProvider metadataProvider) {
      this.accessRuleProviders = accessRuleProviders;
      this.groupProvider = groupProvider;
      this.metadataProvider = metadataProvider;
    }
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
