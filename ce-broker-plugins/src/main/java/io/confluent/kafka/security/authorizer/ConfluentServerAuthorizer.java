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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import kafka.security.auth.Operation;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import kafka.security.authorizer.AuthorizerUtils;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

public class ConfluentServerAuthorizer extends EmbeddedAuthorizer implements Authorizer,
    Reconfigurable {

  private static final Set<String> UNSCOPED_PROVIDERS =
      Utils.mkSet(AccessRuleProviders.ACL.name(), AccessRuleProviders.MULTI_TENANT.name());

  private static final String METRIC_GROUP = "confluent.license";
  private static final LicenseValidator DUMMY_LICENSE_VALIDATOR = new DummyLicenseValidator();
  private static final Set<Class<? extends ConfluentServerAuthorizer>> LICENSE_FREE_AUTHORIZERS =
      Collections.singleton(MultiTenantAuthorizer.class);
  private static final Set<Class<? extends ConfluentServerAuthorizer>> LEGACY_AUTHORIZERS =
      Collections.singleton(LdapAuthorizer.class);

  private final Time time;
  private LicenseValidator licenseValidator;
  private Authorizer aclAuthorizer;
  private Map<String, Object> configs;

  public ConfluentServerAuthorizer() {
    this(Time.SYSTEM);
  }

  public ConfluentServerAuthorizer(Time time) {
    this.time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    this.configs = new HashMap<>(configs);
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    if (auditLogProvider() != null)
      return auditLogProvider().reconfigurableConfigs();
    else
      return Collections.emptySet();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    if (auditLogProvider() != null)
      auditLogProvider().validateReconfiguration(configs);
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    if (auditLogProvider() != null)
      auditLogProvider().reconfigure(configs);
  }

  // Visibility for tests
  public void configureServerInfo(AuthorizerServerInfo serverInfo) {
    super.configureServerInfo(serverInfo);

    Optional<Authorizer> aclProvider = accessRuleProviders().stream()
        .filter(a -> a instanceof AclProvider)
        .findFirst()
        .map(a -> (Authorizer) a);

    if (!aclProvider.isPresent()) {
      aclProvider = accessRuleProviders().stream()
          .filter(a -> a instanceof Authorizer)
          .findFirst()
          .map(a -> (Authorizer) a);
    }
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
  public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
    configureServerInfo(serverInfo);
    CompletableFuture<Void> startFuture = super.start(interBrokerClientConfigs(serverInfo));

    Map<Endpoint, CompletableFuture<Void>> futures = new HashMap<>(serverInfo.endpoints().size());
    serverInfo.endpoints().forEach(endpoint -> {
      if (endpoint.equals(serverInfo.interBrokerEndpoint())) {
        futures.put(endpoint, CompletableFuture.completedFuture(null));
      } else {
        futures.put(endpoint, startFuture);
      }
    });
    return futures;
  }

  @Override
  public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext,
      List<org.apache.kafka.server.authorizer.Action> actions) {
    return actions.stream().map(action -> {
      boolean allowed = authorize(requestContext,
          Operation$.MODULE$.fromJava(action.operation()),
          AuthorizerUtils.convertToResource(action.resourcePattern()));
      return allowed ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED;
    }).collect(Collectors.toList());
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(
      AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
    return aclAuthorizer.createAcls(requestContext, aclBindings);
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
      AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
    return aclAuthorizer.deleteAcls(requestContext, aclBindingFilters);
  }

  @Override
  public Iterable<AclBinding> acls(AclBindingFilter filter) {
    return aclAuthorizer.acls(filter);
  }

  public boolean authorize(AuthorizableRequestContext requestContext, Operation operation, Resource resource) {
    if (ready())
      licenseValidator.verifyLicense();

    if (resource.patternType() != PatternType.LITERAL) {
      throw new IllegalArgumentException("Only literal resources are supported, got: "
          + resource.patternType());
    }

    if (allowBrokerUsersOnInterBrokerListener(requestContext, requestContext.principal())) {
      return true;
    }

    Action action = new Action(scope(),
                               AclMapper.resourceType(resource.resourceType()),
                               resource.name(),
                               AclMapper.operation(operation));

    List<AuthorizeResult> result = super.authorize(
        io.confluent.security.authorizer.utils.AuthorizerUtils.kafkaRequestContext(requestContext),
        Collections.singletonList(action));
    return result.get(0) == AuthorizeResult.ALLOWED;
  }

  @Override
  public void close() {
    log.debug("Closing Kafka authorizer");
    super.close();
    try {
      if (licenseValidator != null)
        licenseValidator.close();
    } catch (Exception e) {
      log.error("Failed to close license validator", e);
    }
  }

  private boolean allowBrokerUsersOnInterBrokerListener(AuthorizableRequestContext requestContext, KafkaPrincipal principal) {
    if (interBrokerListener.equals(requestContext.listener()) && brokerUsers.contains(principal)) {
      log.debug("principal = {} is a broker user, allowing operation without checking any providers.", principal);
      return true;
    }
    return false;
  }

  private Map<String, Object> interBrokerClientConfigs(AuthorizerServerInfo serverInfo) {
    Map<String, Object>  clientConfigs = new HashMap<>();
    Endpoint endpoint = serverInfo.interBrokerEndpoint();
    ListenerName listenerName = new ListenerName(endpoint.listener());
    String listenerPrefix = listenerName.configPrefix();
    clientConfigs.putAll(configs);
    updatePrefixedConfigs(configs, clientConfigs, listenerPrefix);

    SecurityProtocol securityProtocol = serverInfo.interBrokerEndpoint().securityProtocol();
    if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
      String mechanism = (String) configs.get(KafkaConfig$.MODULE$.SaslMechanismInterBrokerProtocolProp());
      clientConfigs.put(SaslConfigs.SASL_MECHANISM, mechanism);
      String mechanismPrefix = listenerName.saslMechanismConfigPrefix(mechanism);
      updatePrefixedConfigs(configs, clientConfigs, mechanismPrefix);
    }
    clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, endpoint.host() + ":" + endpoint.port());
    clientConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
    clientConfigs.put(KafkaConfig$.MODULE$.BrokerIdProp(), serverInfo.brokerId());
    return clientConfigs;
  }

  private void updatePrefixedConfigs(Map<String, Object> srcConfigs, Map<String, Object> dstConfigs, String prefix) {
    srcConfigs.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .forEach(e -> {
          dstConfigs.remove(e.getKey());
          dstConfigs.put(e.getKey().substring(prefix.length()), e.getValue());
        });
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
   * New-style Kafka-topic-based license management is used for ConfluentServerAuthorizer
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
      if (auditLogProvider() != null && auditLogProvider().needsLicense())
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
              + " user-principal based authorization using 'kafka.security.authorizer.AclAuthorizer'"
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
    public Map<Endpoint, CompletableFuture<Void>> start(AuthorizerServerInfo serverInfo) {
      return Collections.emptyMap();
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext,
        List<org.apache.kafka.server.authorizer.Action> actions) {
      throw new IllegalStateException("Authorization not supported by this provider");
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(
        AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
      throw EXCEPTION;
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
        AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
      throw EXCEPTION;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
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
