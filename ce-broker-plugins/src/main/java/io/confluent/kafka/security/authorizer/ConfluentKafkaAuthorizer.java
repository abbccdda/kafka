// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.common.license.LicenseValidator;
import io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer;
import io.confluent.kafka.security.authorizer.acl.AclMapper;
import io.confluent.kafka.security.authorizer.acl.AclProvider;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.Provider;
import io.confluent.license.validator.ConfluentLicenseValidator;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import kafka.network.RequestChannel;
import kafka.network.RequestChannel.Session;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.AuthorizerWithKafkaStore;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;


public class ConfluentKafkaAuthorizer extends EmbeddedAuthorizer implements AuthorizerWithKafkaStore {

  private static final Set<String> UNSCOPED_PROVIDERS =
      Utils.mkSet(AccessRuleProviders.ACL.name(), AccessRuleProviders.MULTI_TENANT.name());
  private static final Set<Class<? extends ConfluentKafkaAuthorizer>> LICENSE_FREE_AUTHORIZERS =
      Collections.singleton(MultiTenantAuthorizer.class);

  private Authorizer aclAuthorizer;
  private volatile CompletableFuture<Void> readyFuture;

  public ConfluentKafkaAuthorizer() {
    this(Time.SYSTEM);
  }

  public ConfluentKafkaAuthorizer(Time time) {
    super(time);
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

    readyFuture = start();
  }

  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {

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
  }

  @Override
  public CompletableFuture<Void> readyFuture() {
    return readyFuture;
  }

  // Make this final so that license validation cannot be trivially overridden
  @Override
  protected final LicenseValidator licenseValidator() {
    if (LICENSE_FREE_AUTHORIZERS.contains(this.getClass()))
      return null;
    else
      return new ConfluentLicenseValidator();
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
}
