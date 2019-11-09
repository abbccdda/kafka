// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;

public class ConfluentBuiltInProviders {

  public enum AccessRuleProviders {
    ZK_ACL,        // Broker's ACL provider consistent with SimpleAclAuthorizer
    MULTI_TENANT,  // Multi-tenant ACL provider for CCloud
    CONFLUENT      // RBAC metadata provider that uses centralized auth topic with roles and groups
  }

  public enum GroupProviders {
    LDAP,          // LDAP group provider that directly obtains groups from LDAP
    CONFLUENT,     // MDS-based metadata provider that uses centralized auth topic with roles and groups
    NONE           // Groups disabled
  }

  public enum MetadataProviders {
    CONFLUENT,          // Embedded Metadata Server with REST interface
    NONE           // Embedded Metadata Service not enabled on the broker
  }

  private static final Map<String, String> OLD_PROVIDER_NAMES = Utils.mkMap(
      Utils.mkEntry("ACL", AccessRuleProviders.ZK_ACL.name()),
      Utils.mkEntry("RBAC", AccessRuleProviders.CONFLUENT.name()));

  public static Set<String> builtInAccessRuleProviders() {
    return Utils.mkSet(AccessRuleProviders.values()).stream()
        .map(AccessRuleProviders::name).collect(Collectors.toSet());
  }

  public static List<AccessRuleProvider> loadAccessRuleProviders(List<String> names) {
    Map<String, AccessRuleProvider> authProviders = new HashMap<>(names.size());
    ServiceLoader<AccessRuleProvider> providers = ServiceLoader.load(AccessRuleProvider.class);
    for (AccessRuleProvider provider : providers) {
      String name = provider.providerName();
      if (names.contains(name))
        authProviders.putIfAbsent(name, provider);
      if (authProviders.size() == names.size())
        break;
    }
    if (authProviders.size() != names.size()) {
      Set<String> remainingNames = new HashSet<>(names);
      remainingNames.removeAll(authProviders.keySet());
      if (OLD_PROVIDER_NAMES.keySet().containsAll(remainingNames)) {

        StringBuilder error = new StringBuilder();
        remainingNames.forEach(name -> {
          error.append(name);
          error.append(" is supported by the provider named ");
          error.append(OLD_PROVIDER_NAMES.get(name));
          error.append(", ");
        });
        error.append("you should configure '");
        error.append(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP);
        error.append('=');
        Collection<String> newProviders = names.stream().map(OLD_PROVIDER_NAMES::get).collect(Collectors.toSet());
        error.append(Utils.join(newProviders, ","));
        error.append("'");
        throw new ConfigException(error.toString());
      } else {
        throw new ConfigException("Access rule provider not found for " + remainingNames);
      }
    }
    return names.stream().map(authProviders::get).collect(Collectors.toList());
  }

  public static GroupProvider loadGroupProvider(Map<String, ?> configs) {
    ServiceLoader<GroupProvider> providers = ServiceLoader.load(GroupProvider.class);
    for (GroupProvider provider : providers) {
      if (providerEnabled(provider, configs) && provider.providerConfigured(configs)) {
        return provider;
      }
    }
    return new EmptyGroupProvider();
  }

  public static MetadataProvider loadMetadataProvider(Map<String, ?> configs) {
    ServiceLoader<MetadataProvider> providers = ServiceLoader.load(MetadataProvider.class);
    for (MetadataProvider provider : providers) {
      if (providerEnabled(provider, configs) && provider.providerConfigured(configs)) {
        return provider;
      }
    }
    return new EmptyMetadataProvider();
  }

  public static AuditLogProvider loadAuditLogProvider(Map<String, ?> configs) {
    ServiceLoader<AuditLogProvider> providers = ServiceLoader.load(AuditLogProvider.class);
    for (AuditLogProvider provider : providers) {
      if (provider.providerConfigured(configs)) {
        return provider;
      }
    }
    return new DefaultAuditLogProvider();
  }

  /**
   * Provider selection without using explicit provider configs for metadata providers.
   *   - Only LdapAuthorizer uses the LDAP group provider. RBAC uses groups from metadata topic
   *     populated from LDAP by RbacProvider
   *   - For other providers, only load providers with the same name as access rule providers
   *     e.g. load RBAC metadata/group provider if RBAC is enabled for access rules
   *   - If this method returns true, caller also checks if the provider has been configured.
   *     e.g. Metadata server is created only if listener is configured
   */
  private static boolean providerEnabled(Provider provider, Map<String, ?> configs) {
    String providerName = provider.providerName();
    Object accessRuleProviders = configs.get(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP);
    if (provider.providerName().equals("LDAP"))
      return String.valueOf(configs.get("authorizer.class.name")).endsWith(".LdapAuthorizer");
    else if (accessRuleProviders == null)
      return false;
    else if (accessRuleProviders instanceof String)
      return Arrays.stream(((String) accessRuleProviders).split(",")).anyMatch(providerName::equals);
    else if (accessRuleProviders instanceof List)
      return ((List<?>) accessRuleProviders).stream().anyMatch(providerName::equals);
    else
      return false;
  }

  private static class EmptyGroupProvider implements GroupProvider {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal) {
      return Collections.emptySet();
    }

    @Override
    public boolean usesMetadataFromThisKafkaCluster() {
      return false;
    }

    @Override
    public String providerName() {
      return GroupProviders.NONE.name();
    }

    @Override
    public boolean providerConfigured(Map<String, ?> configs) {
      return true;
    }

    @Override
    public void close() {
    }
  }

  private static class EmptyMetadataProvider implements MetadataProvider {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public String providerName() {
      return MetadataProviders.NONE.name();
    }

    @Override
    public boolean usesMetadataFromThisKafkaCluster() {
      return false;
    }

    @Override
    public boolean providerConfigured(Map<String, ?> configs) {
      return true;
    }

    @Override
    public void close() {
    }
  }
}