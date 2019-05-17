// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

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
    ACL,           // Broker's ACL provider consistent with SimpleAclAuthorizer
    MULTI_TENANT,  // Multi-tenant ACL provider for CCloud
    RBAC           // RBAC metadata provider that uses centralized auth topic with roles and groups
  }

  public enum GroupProviders {
    LDAP,          // LDAP group provider that directly obtains groups from LDAP
    RBAC,          // RBAC metadata provider that uses centralized auth topic with roles and groups
    NONE           // Groups disabled
  }

  public enum MetadataProviders {
    RBAC,          // Embedded Metadata Server with REST interface
    NONE           // Embedded Metadata Service not enabled on the broker
  }

  public static Set<String> builtInAccessRuleProviders() {
    return Utils.mkSet(AccessRuleProviders.values()).stream()
        .map(AccessRuleProviders::name).collect(Collectors.toSet());
  }

  public static Set<String> builtInGroupProviders() {
    return Utils.mkSet(GroupProviders.values()).stream()
        .map(GroupProviders::name).collect(Collectors.toSet());
  }

  public static Set<String> builtInMetadataProviders() {
    return Utils.mkSet(MetadataProviders.values()).stream()
        .map(MetadataProviders::name).collect(Collectors.toSet());
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
      throw new ConfigException("Provider not found for " + remainingNames);
    }
    return names.stream().map(authProviders::get).collect(Collectors.toList());
  }

  public static GroupProvider loadGroupProvider(String name) {
    if (name.equals(GroupProviders.NONE.name()))
      return new EmptyGroupProvider();

    GroupProvider groupProvider = null;
    ServiceLoader<GroupProvider> providers = ServiceLoader.load(GroupProvider.class);
    for (GroupProvider provider : providers) {
      if (provider.providerName().equals(name)) {
        groupProvider = provider;
        break;
      }
    }
    if (groupProvider == null)
      throw new ConfigException("Group provider not found for " + name);
    return groupProvider;
  }

  public static MetadataProvider loadMetadataProvider(String name) {
    if (name.equals(MetadataProviders.NONE.name()))
      return new EmptyMetadataProvider();

    MetadataProvider metadataProvider = null;
    ServiceLoader<MetadataProvider> providers = ServiceLoader.load(MetadataProvider.class);
    for (MetadataProvider provider : providers) {
      if (provider.providerName().equals(name)) {
        metadataProvider = provider;
        break;
      }
    }
    if (metadataProvider == null)
      throw new ConfigException("Metadata provider not found for " + name);
    return metadataProvider;
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
    public boolean needsLicense() {
      return false;
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
    public boolean needsLicense() {
      return false;
    }

    @Override
    public void close() {
    }
  }
}