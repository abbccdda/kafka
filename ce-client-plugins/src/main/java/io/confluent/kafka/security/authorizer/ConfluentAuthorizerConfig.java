// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.MetadataProviders;
import io.confluent.kafka.security.authorizer.provider.GroupProvider;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.GroupProviders;
import io.confluent.kafka.security.authorizer.provider.MetadataProvider;
import io.confluent.kafka.security.authorizer.provider.Provider;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

public class ConfluentAuthorizerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String SCOPE_PROP = "confluent.authorizer.scope";
  private static final String SCOPE_DEFAULT = "";
  private static final String SCOPE_DOC = "The scope used for role-based authorization"
      + " of requests on the broker. This should be broker's cluster scope."
      + " This may be empty if RBAC provider is not enabled.";

  public static final String GROUP_PROVIDER_PROP = "confluent.authorizer.group.provider";
  private static final String GROUP_PROVIDER_DEFAULT = GroupProviders.NONE.name();
  private static final String GROUP_PROVIDER_DOC = "Group provider for the authorizer to map users to groups. "
      + " Supported providers are " + ConfluentBuiltInProviders.builtInGroupProviders()
      + ". Group-based authorization is disabled by default.";

  public static final String ACCESS_RULE_PROVIDERS_PROP = "confluent.authorizer.access.rule.providers";
  private static final String ACCESS_RULE_PROVIDERS_DEFAULT = AccessRuleProviders.ACL.name();
  private static final String ACCESS_RULE_PROVIDERS_DOC = "List of access rule providers enabled. "
      + " Access rule providers supported are " + ConfluentBuiltInProviders.builtInAccessRuleProviders()
      + ". ACL-based provider is enabled by default.";

  public static final String METADATA_PROVIDER_PROP = "confluent.authorizer.metadata.provider";
  private static final String METADATA_PROVIDER_DEFAULT = MetadataProviders.NONE.name();
  private static final String METATDATA_PROVIDER_DOC = "Metadata provider that provides authentication "
      + " and authorization metadata for other components using a metadata server embedded in the broker."
      + " Supported providers are " + ConfluentBuiltInProviders.builtInMetadataProviders()
      + ". Metadata servers are disabled by default. Note that the metadata server started by this provider"
      + " enables authorization in other components, but is not used for authorization within this broker.";

  public static final String LICENSE_PROP = "confluent.license";
  private static final String LICENSE_DEFAULT = "";
  private static final String LICENSE_DOC = "License for Confluent plugins.";


  // SimpleAclAuthorizer configs

  public static final String ALLOW_IF_NO_ACLS_PROP = "allow.everyone.if.no.acl.found";
  private static final boolean ALLOW_IF_NO_ACLS_DEFAULT = false;
  private static final String ALLOW_IF_NO_ACLS_DOC =
      "Boolean flag that indicates if everyone is allowed access to a resource if no ACL is found.";

  public static final String SUPER_USERS_PROP = "super.users";
  private static final String SUPER_USERS_DEFAULT = "";
  private static final String SUPER_USERS_DOC = "Semicolon-separated list of principals of"
      + " super users who are allowed access to all resources.";

  static {
    CONFIG = new ConfigDef()
        .define(SCOPE_PROP, Type.STRING, SCOPE_DEFAULT,
            Importance.HIGH, SCOPE_DOC)
        .define(ALLOW_IF_NO_ACLS_PROP, Type.BOOLEAN, ALLOW_IF_NO_ACLS_DEFAULT,
            Importance.MEDIUM, ALLOW_IF_NO_ACLS_DOC)
        .define(SUPER_USERS_PROP, Type.STRING, SUPER_USERS_DEFAULT,
            Importance.MEDIUM, SUPER_USERS_DOC)
        .define(ACCESS_RULE_PROVIDERS_PROP, Type.LIST, ACCESS_RULE_PROVIDERS_DEFAULT,
            Importance.MEDIUM, ACCESS_RULE_PROVIDERS_DOC)
        .define(GROUP_PROVIDER_PROP, Type.STRING, GROUP_PROVIDER_DEFAULT,
            Importance.MEDIUM, GROUP_PROVIDER_DOC)
        .define(METADATA_PROVIDER_PROP, Type.STRING, METADATA_PROVIDER_DEFAULT,
            Importance.MEDIUM, METATDATA_PROVIDER_DOC)
        .define(LICENSE_PROP, Type.STRING, LICENSE_DEFAULT,
            Importance.HIGH, LICENSE_DOC);
  }
  public final boolean allowEveryoneIfNoAcl;
  public final String scope;

  public ConfluentAuthorizerConfig(Map<?, ?> props) {
    super(CONFIG, props);

    scope = getString(SCOPE_PROP);
    allowEveryoneIfNoAcl = getBoolean(ALLOW_IF_NO_ACLS_PROP);

    if (getList(ACCESS_RULE_PROVIDERS_PROP).isEmpty())
      throw new ConfigException("No access rule providers specified");
  }

  public final Providers createProviders() {
    List<String> authProviderNames = getList(ACCESS_RULE_PROVIDERS_PROP);
    // Multitenant ACLs are included in the MultiTenantProvider, so include only the MultiTenantProvider
    if (authProviderNames.contains(AccessRuleProviders.ACL.name())
        && authProviderNames.contains(AccessRuleProviders.MULTI_TENANT.name())) {
      authProviderNames = new ArrayList<>(authProviderNames);
      authProviderNames.remove(AccessRuleProviders.ACL.name());
    }
    if (authProviderNames.isEmpty())
      throw new ConfigException("No access rule providers specified");

    List<AccessRuleProvider> accessRuleProviders =
        ConfluentBuiltInProviders.loadAccessRuleProviders(authProviderNames);
    Set<Provider> providers = new HashSet<>(accessRuleProviders);

    String groupFeature = getString(GROUP_PROVIDER_PROP);
    String groupProviderName = groupFeature == null || groupFeature.isEmpty()
        ? GroupProviders.NONE.name() : groupFeature;
    GroupProvider groupProvider = createProvider(GroupProvider.class,
        groupProviderName,
        ConfluentBuiltInProviders::loadGroupProvider,
        providers);
    providers.add(groupProvider);

    String metadataFeature = getString(METADATA_PROVIDER_PROP);
    String metadataProviderName = metadataFeature == null || metadataFeature.isEmpty()
        ? MetadataProviders.NONE.name() : metadataFeature;
    MetadataProvider metadataProvider = createProvider(MetadataProvider.class,
        metadataProviderName,
        ConfluentBuiltInProviders::loadMetadataProvider,
        providers);
    providers.add(metadataProvider);

    providers.forEach(provider -> provider.configure(originals()));

    return new Providers(accessRuleProviders, groupProvider, metadataProvider);
  }

  @SuppressWarnings("unchecked")
  private <T extends Provider> T createProvider(Class<T> providerClass,
                                                String providerName,
                                                Function<String, T> creator,
                                                Collection<? extends Provider> otherProviders) {
    for (Provider provider : otherProviders) {
      if (provider.providerName().equals(providerName) && providerClass.isInstance(provider))
        return (T) provider;
    }
    return creator.apply(providerName);
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
