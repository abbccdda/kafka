// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.security.authorizer.acl.AclMapper;
import io.confluent.kafka.security.authorizer.acl.AclProvider;
import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.license.InvalidLicenseException;
import io.confluent.license.validator.LegacyLicenseValidator;
import io.confluent.license.validator.LicenseConfig;
import io.confluent.license.validator.LicenseValidator;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.Provider;
import io.confluent.license.validator.ConfluentLicenseValidator;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.network.RequestChannel;
import kafka.network.RequestChannel.Session;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.AuthorizerWithKafkaStore;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;


public class ConfluentKafkaAuthorizer extends EmbeddedAuthorizer implements AuthorizerWithKafkaStore {

  private static final Set<String> UNSCOPED_PROVIDERS =
      Utils.mkSet(AccessRuleProviders.ACL.name(), AccessRuleProviders.MULTI_TENANT.name());

  private static final String METRIC_GROUP = "confluent.license";
  private static final LicenseValidator DUMMY_LICENSE_VALIDATOR = new DummyLicenseValidator();
  private static final Set<Class<? extends ConfluentKafkaAuthorizer>> LICENSE_FREE_AUTHORIZERS =
      Collections.singleton(MultiTenantAuthorizer.class);
  private static final Set<Class<? extends ConfluentKafkaAuthorizer>> LEGACY_AUTHORIZERS =
      Collections.singleton(LdapAuthorizer.class);

  private final Time time;
  private LicenseValidator licenseValidator;
  private Authorizer aclAuthorizer;

  public ConfluentKafkaAuthorizer() {
    this(Time.SYSTEM);
  }

  public ConfluentKafkaAuthorizer(Time time) {
    this.time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    Optional<Authorizer> aclProvider = accessRuleProviders().stream()
        .filter(a -> a instanceof AclProvider)
        .findFirst()
        .map(a -> (Authorizer) a);
    aclAuthorizer = aclProvider.orElse(new AclErrorProvider());

    // Embedded authorizer used in metadata server can use an empty scope since scopes used
    // in authorization are provided by the remote client. For broker authorizer, the scope
    // of the cluster is required if using providers other than ACL providers.
    if (scope().clusters().isEmpty()) {
      Set<String> scopedProviders = accessRuleProviders().stream()
          .map(Provider::providerName)
          .filter(a -> !UNSCOPED_PROVIDERS.contains(a))
          .collect(Collectors.toSet());

      if (!scopedProviders.isEmpty())
        throw new ConfigException("Scope not provided for broker providers: " + scopedProviders);
    }
    createLicenseValidator();
  }

  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {
    if (ready())
      licenseValidator.verifyLicense();

    if (resource.patternType() != PatternType.LITERAL) {
      throw new IllegalArgumentException("Only literal resources are supported, got: "
          + resource.patternType());
    }
    Action action = new Action(scope(),
                               AclMapper.resourceType(resource.resourceType()),
                               resource.name(),
                               AclMapper.operation(operation));
    String host = session.clientAddress().getHostAddress();

    List<AuthorizeResult> result = super.authorize(session.principal(), host, Collections.singletonList(action));
    return result.get(0) == AuthorizeResult.ALLOWED;
  }

  @Override
  public void addAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
    aclAuthorizer.addAcls(acls, resource);
  }

  @Override
  public boolean removeAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
    return aclAuthorizer.removeAcls(acls, resource);
  }

  @Override
  public boolean removeAcls(Resource resource) {
    return aclAuthorizer.removeAcls(resource);
  }

  @Override
  public scala.collection.immutable.Set<Acl> getAcls(Resource resource) {
    return aclAuthorizer.getAcls(resource);
  }

  @Override
  public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls(KafkaPrincipal principal) {
    return aclAuthorizer.getAcls(principal);
  }

  @Override
  public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls() {
    return aclAuthorizer.getAcls();
  }

  @Override
  public void close() {
    log.debug("Closing Kafka authorizer");
    super.close();
    try {
      licenseValidator.close();
    } catch (Exception e) {
      log.error("Failed to close license validator", e);
    }
  }

  /**
   * Creates a license validator.
   *
   * License validation is disabled if:
   * 1) Proprietary features are not used (e.g. only ACLs are enabled) OR
   * 2) This authorizer is license-free (e.g. Tenant authorizer for CCloud)
   *
   * Legacy license validation without Kafka topics is used for the legacy LdapAuthorizer
   * to ensure that we can continue to support clusters with single listeners without a separate
   * metadata cluster.
   *
   * New-style Kafka-topic-based license management is used for ConfluentKafkaAuthorizer
   * with proprietary features like RBAC. These have the same restrictions as metadata service.
   * In single-cluster deployments, metadata cluster brokers must be configured with a separate
   * inter-broker listener where broker requests can be authorized using AclProvider.
   */
  private void createLicenseValidator() {
    if (LICENSE_FREE_AUTHORIZERS.contains(this.getClass()))
      licenseValidator = DUMMY_LICENSE_VALIDATOR;
    else if (LEGACY_AUTHORIZERS.contains(this.getClass()))
      licenseValidator = new LegacyLicenseValidator(time);
    else {
      boolean needsLicense = accessRuleProviders().stream().anyMatch(Provider::needsLicense);
      if (groupProvider() != null && groupProvider().needsLicense())
        needsLicense = true;
      if (metadataProvider() != null && metadataProvider().needsLicense())
        needsLicense = true;
      if (needsLicense)
        licenseValidator = new ConfluentLicenseValidator(time);
      else
        licenseValidator = DUMMY_LICENSE_VALIDATOR;
    }
  }

  // Make this final so that license validation cannot be trivially overridden
  protected final void initializeAndValidateLicense(Map<String, ?> configs) {
    String id = "broker-" + configs.get("broker.id");
    Map<String, Object> licenseConfigs = new HashMap<>(configs);
    LicenseConfig config = new LicenseConfig(id, configs);
    replacePrefix(config, licenseConfigs, "confluent.metadata.", LicenseConfig.PREFIX);
    replacePrefix(config, licenseConfigs, "confluent.metadata.consumer.", LicenseConfig.CONSUMER_PREFIX);
    replacePrefix(config, licenseConfigs, "confluent.metadata.producer.", LicenseConfig.PRODUCER_PREFIX);
    licenseValidator.configure(licenseConfigs);

    String licensePropName = licensePropName();
    String license = (String) configs.get(licensePropName);
    try {
      licenseValidator.initializeAndVerify(license, licenseStatusMetricGroup(), id);
    } catch (InvalidLicenseException e) {
      throw new KafkaException(
          String.format("Confluent Authorizer license validation failed."
              + " Please specify a valid license in the config " + licensePropName
              + " to enable authorization using %s. Kafka brokers may be started with basic"
              + " user-principal based authorization using 'kafka.security.auth.SimpleAclAuthorizer'"
              + " without a license.", this.getClass().getName()), e);
    }
  }

  private void replacePrefix(AbstractConfig srcConfig, Map<String, Object> dstConfigs, String srcPrefix, String dstPrefix) {
    Map<String, Object> prefixedConfigs = srcConfig.originalsWithPrefix(srcPrefix);
    prefixedConfigs.forEach((k, v) -> {
      dstConfigs.remove(srcPrefix + k);
      dstConfigs.putIfAbsent(dstPrefix + k, v);
    });
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom property
  protected String licensePropName() {
    return ConfluentAuthorizerConfig.LICENSE_PROP;
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom metric
  protected String licenseStatusMetricGroup() {
    return METRIC_GROUP;
  }

  private static class AclErrorProvider implements Authorizer {

    private static final InvalidRequestException EXCEPTION =
        new InvalidRequestException("ACL-based authorization is disabled");

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public boolean authorize(Session session, Operation operation, Resource resource) {
      throw new IllegalStateException("Authprization not supported by this provider");
    }

    @Override
    public void addAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public boolean removeAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public boolean removeAcls(Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public scala.collection.immutable.Set<Acl> getAcls(Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls(KafkaPrincipal principal) {
      throw EXCEPTION;
    }

    @Override
    public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls() {
      throw EXCEPTION;
    }

    @Override
    public void close() {
    }
  }

  private static class DummyLicenseValidator implements LicenseValidator {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void initializeAndVerify(String license, String metricGroup, String componentId) {
    }

    @Override
    public boolean verifyLicense() {
      return true;
    }

    @Override
    public void close() {
    }
  }
}
