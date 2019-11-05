// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.acl;

import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import java.util.function.Supplier;
import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AclUpdateListener;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.requests.ApiError;
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

  private final String clusterId;
  private final Supplier<Integer> brokerIdSupplier;
  private int writerBrokerId;

  public MdsAclMigration(final String clusterId, Supplier<Integer> brokerIdSupplier) {
    this.clusterId =  clusterId;
    this.brokerIdSupplier = brokerIdSupplier;
  }

  public void migrate(final Map<String, ?> configs,
                      Authorizer aclAuthorizer,
                      ConfluentAdmin mdsAdminClient) {

    try {
      Integer brokerId;
      while (true) {
        brokerId = brokerIdSupplier.get();
        while (brokerId == null) {
          Thread.sleep(10);
          brokerId = brokerIdSupplier.get();
        }
        this.writerBrokerId = brokerId;

        try {
          tryMigrate(configs, aclAuthorizer, mdsAdminClient);
          break;
        } catch (RetriableException e) {
          log.warn("Migration failed, retring", e);
        }
      }
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    }
  }

    /**
     * Migrate ACLs for source authorizer to MDS/centralized store.
     */
  private void tryMigrate(final Map<String, ?> configs,
      Authorizer aclAuthorizer,
      ConfluentAdmin mdsAdminClient) {

    ConfluentAuthorizerConfig confluentAuthorizerConfig = new ConfluentAuthorizerConfig(configs);
    log.info("Starting Acl migration from ZK to metadata service");

    addUpdateListener(aclAuthorizer, mdsAdminClient);
    Iterable<AclBinding> aclBindings = aclAuthorizer.acls(AclBindingFilter.ANY);

    int batchSize = confluentAuthorizerConfig.getInt(ConfluentAuthorizerConfig.ACL_MIGRATION_BATCH_SIZE_PROP);

    List<AclBinding> aclBatch = new ArrayList<>(batchSize);
    int count = 0;

    for (final AclBinding aclBinding : aclBindings) {
      aclBatch.add(aclBinding);
      count++;

      if (count == batchSize) {
        migrateBindings(mdsAdminClient, aclBatch);
        count = 0;
        aclBatch.clear();
      }
    }

    if (!aclBatch.isEmpty()) {
      migrateBindings(mdsAdminClient, aclBatch);
    }

    log.info("Completed Acl migration from ZK to metadata service");
  }

  private void addUpdateListener(final Authorizer aclAuthorizer, final ConfluentAdmin mdsAdminClient) {
    ((AclAuthorizer) aclAuthorizer).registerAclUpdateListener(new AclUpdateListener() {
      @Override
      public void handleUpdate(final ResourcePattern resourcePattern, final Set<AccessControlEntry> aclBindings) {
        log.info("handling ACL updates during migration for resource {}, bindings {}", resourcePattern, aclBindings);
        try {
          //delete all existing bindings for the resource
          AclBindingFilter aclBindingFilter = new AclBindingFilter(resourcePattern.toFilter(),
              AccessControlEntryFilter.ANY);

          try {
            ensureNoRebalance();
            FilterResults deleteResults = mdsAdminClient
                .deleteAcls(Collections.singleton(aclBindingFilter), new DeleteAclsOptions(), clusterId, writerBrokerId)
                .values().get(aclBindingFilter).get();

            Map<AclBinding, ApiException> failedDeleteResults = failedDeleteResults(deleteResults);
            if (!failedDeleteResults.isEmpty()) {
              log.error("Failed to update delete ACLs bindings from metadata service: failed list {}", failedDeleteResults);
            }
          } catch (Throwable t) {
            log.error("Failed to delete ACLs bindings from metadata service: failed filter {}", aclBindingFilter);
          }

          // add the updated bindings
          if (!aclBindings.isEmpty()) {
            List<AclBinding> aclBatch = aclBindings.stream()
                .map(a -> new AclBinding(resourcePattern, a))
                .collect(Collectors.toList());
            Map<AclBinding, ApiError> createResults = createAcls(mdsAdminClient, aclBatch);
            Map<AclBinding, ApiError> failedCreateResults = failedCreateResults(createResults);
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

  private void migrateBindings(final ConfluentAdmin mdsAdminClient, final List<AclBinding> aclBatch) {
    log.info("Starting migrating Acls of batch size {}", aclBatch.size());


    Map<AclBinding, ApiError> createResults = createAcls(mdsAdminClient, aclBatch);
    Map<AclBinding, ApiError> failedBindings = failedCreateResults(createResults);

    if (failedBindings.isEmpty()) {
      log.info("Completed migrating Acls of batch size {}", aclBatch.size());
    } else {
      log.error("Failed to migrate Acls from ZK to metadata service: failed list {}", failedBindings);
      throw new RuntimeException("Failed to migrate Acls from ZK to metadata service.");
    }
  }

  private Map<AclBinding, ApiError> createAcls(final ConfluentAdmin mdsAdminClient, List<AclBinding> aclBatch) {
    ensureNoRebalance();
    return mdsAdminClient.createAcls(aclBatch, new CreateAclsOptions(), clusterId, writerBrokerId).values()
        .entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> {
          try {
            e.getValue().get();
            return ApiError.NONE;
          } catch (Throwable t) {
            return ApiError.fromThrowable(t);
          }
        }));
  }

  private Map<AclBinding, ApiError> failedCreateResults(final Map<AclBinding, ApiError> createAclResults) {
    return createAclResults
        .entrySet()
        .stream()
        .filter(x -> x.getValue() != ApiError.NONE)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<AclBinding, ApiException> failedDeleteResults(FilterResults deleteAclsResult) {
    return deleteAclsResult.values()
        .stream()
        .filter(x -> x.exception() != null)
        .collect(Collectors.toMap(FilterResult::binding, FilterResult::exception));
  }

  private void ensureNoRebalance() {
    if (writerBrokerId != brokerIdSupplier.get())
      throw new RebalanceInProgressException("Writer broker changed, restarting ACL migration");
  }
}
