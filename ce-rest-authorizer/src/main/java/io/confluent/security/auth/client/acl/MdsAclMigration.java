// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.acl;

import io.confluent.security.auth.client.rest.entities.CreateAclsRequest;
import io.confluent.security.auth.client.rest.entities.CreateAclsResult;
import io.confluent.security.auth.client.rest.entities.DeleteAclsRequest;
import io.confluent.security.auth.client.rest.entities.DeleteAclsResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.Scope;
import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AclUpdateListener;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.server.authorizer.Authorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MdsAclMigration {
  private static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  private final Scope scope;

  public MdsAclMigration(final Scope scope) {
    this.scope =  scope;
  }

  /**
   * Migrate ACLs for source authorizer to MDS/centralized store.
   * @param configs
   * @param aclAuthorizer
   */
  public void migrate(final Map<String, ?> configs, Authorizer aclAuthorizer)  {

    ConfluentAuthorizerConfig confluentAuthorizerConfig = new ConfluentAuthorizerConfig(configs);
    log.info("Starting Acl migration from ZK to metadata service");
    final MdsAclClient mdsAclClient = new MdsAclClient();
    mdsAclClient.configure(configs);

    addUpdateListener(aclAuthorizer, mdsAclClient);
    Iterable<AclBinding> aclBindings = aclAuthorizer.acls(AclBindingFilter.ANY);

    int batchSize = confluentAuthorizerConfig.getInt(ConfluentAuthorizerConfig.ACL_MIGRATION_BATCH_SIZE_PROP);

    List<AclBinding> aclBatch = new ArrayList<>(batchSize);
    int count = 0;

    for (final AclBinding aclBinding : aclBindings) {
      aclBatch.add(aclBinding);
      count++;

      if (count == batchSize) {
        migrateBindings(mdsAclClient, aclBatch);
        count = 0;
        aclBatch.clear();
      }
    }

    if (!aclBatch.isEmpty()) {
      migrateBindings(mdsAclClient, aclBatch);
    }

    log.info("Completed Acl migration from ZK to metadata service");
  }

  private void addUpdateListener(final Authorizer aclAuthorizer, final MdsAclClient mdsAclClient) {
    ((AclAuthorizer) aclAuthorizer).registerAclUpdateListener(new AclUpdateListener() {
      @Override
      public void handleUpdate(final ResourcePattern resourcePattern, final Set<AccessControlEntry> aclBindings) {
        log.info("handling ACL updates during migration for resource {}, bindings {}", resourcePattern, aclBindings);
        try {
          //delete all existing bindings for the resource
          AclBindingFilter aclBindingFilter = new AclBindingFilter(resourcePattern.toFilter(),
              AccessControlEntryFilter.ANY);
          DeleteAclsRequest deleteAclsRequest = new DeleteAclsRequest(scope, Collections.singleton(aclBindingFilter));
          DeleteAclsResult deleteAclsResult = mdsAclClient.deleteAcls(deleteAclsRequest);
          Map<AclBindingFilter, DeleteAclsResult.DeleteResult> failedDeleteResults = failedDeleteResults(deleteAclsResult);

          if (!failedDeleteResults.isEmpty()) {
            log.error("Failed to update delete ACLs bindings from metadata service: failed list {}", failedDeleteResults);
          }

          // add the updated bindings
          if (!aclBindings.isEmpty()) {
            List<AclBinding> aclBatch = aclBindings.stream()
                .map(a -> new AclBinding(resourcePattern, a))
                .collect(Collectors.toList());
            CreateAclsRequest createAclsRequest = new CreateAclsRequest(scope, aclBatch);
            CreateAclsResult createAclsResult = mdsAclClient.createAcls(createAclsRequest);
            Map<AclBinding, CreateAclsResult.CreateResult> failedCreateResults = failedCreateResults(createAclsResult);
            if (!failedCreateResults.isEmpty()) {
              log.error("Failed to update ACls to metadata service: failed list {}", failedCreateResults);
            }
          }
        } catch (Exception e) {
          log.error("Error while handling ACL updates", e);
        }
      }
    });
  }

  private void migrateBindings(final MdsAclClient aclClient, final List<AclBinding> aclBatch) {
    log.info("Starting migrating Acls of batch size {}", aclBatch.size());

    CreateAclsRequest createAclsRequest = new CreateAclsRequest(scope, aclBatch);
    CreateAclsResult createAclsResult = aclClient.createAcls(createAclsRequest);

    Map<AclBinding, CreateAclsResult.CreateResult> failedBindings = failedCreateResults(createAclsResult);

    if (failedBindings.isEmpty()) {
      log.info("Completed migrating Acls of batch size {}", aclBatch.size());
    } else {
      log.error("Failed to migrate Acls from ZK to metadata service: failed list {}", failedBindings);
      throw new RuntimeException("Failed to migrate Acls from ZK to metadata service.");
    }
  }

  private Map<AclBinding, CreateAclsResult.CreateResult> failedCreateResults(final CreateAclsResult createAclsResult) {
    return createAclsResult.resultMap
        .entrySet()
        .stream()
        .filter(x -> !x.getValue().success)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<AclBindingFilter, DeleteAclsResult.DeleteResult> failedDeleteResults(final DeleteAclsResult deleteAclsResult) {
    return deleteAclsResult.resultMap
        .entrySet()
        .stream()
        .filter(x -> !x.getValue().success)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
