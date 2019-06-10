// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RoleBinding;
import io.confluent.security.store.MetadataStoreException;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.test.utils.LdapTestUtils;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaAuthStoreTest {

  private final Time time = new MockTime();
  private final Scope clusterA = new Scope.Builder("testOrg").withKafkaCluster("clusterA").build();
  private final int storeNodeId = 1;

  private RbacRoles rbacRoles;
  private MockAuthStore authStore;
  private KafkaAuthWriter authWriter;
  private DefaultAuthCache authCache;
  private MiniKdcWithLdapService miniKdcWithLdapService;

  @Before
  public void setUp() throws Exception {
    rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
    if (miniKdcWithLdapService != null)
      miniKdcWithLdapService.shutdown();
    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testConcurrency() throws Exception {
    createAuthStore();
    startAuthService();
    assertTrue(authStore.isMasterWriter());

    authStore.configureDelays(1, 2);
    for (int i = 0; i < 100; i++) {
      authWriter.addClusterRoleBinding(principal("user" + i), "Operator", clusterA);
    }
    TestUtils.waitForCondition(() -> authCache.rbacRoleBindings(clusterA).size() == 100,
        "Roles not assigned");
    for (int i = 0; i < 100; i++) {
      verifyRole("user" + i, "Operator");
    }

    authStore.configureDelays(2, 1);
    for (int i = 0; i < 100; i++) {
      authWriter.addClusterRoleBinding(principal("user" + i), "ClusterAdmin", clusterA);
    }
    TestUtils.waitForCondition(() -> authCache.rbacRoleBindings(clusterA).size() == 200,
        "Roles not assigned");

    for (int i = 0; i < 100; i++) {
      verifyRole("user" + i,  "ClusterAdmin");
    }
  }

  @Test
  public void testUpdateFromDifferentNode() throws Exception {
    createAuthStore();
    startAuthService();

    authStore.makeMasterWriter(storeNodeId + 1);
    assertFalse(authStore.isMasterWriter());
    int startOffset = authStore.producer.history().size();
    for (int i = 0; i < 100; i++) {
      ConsumerRecord<AuthKey, AuthValue> record = new ConsumerRecord<>(
          KafkaAuthStore.AUTH_TOPIC, 0, startOffset + i,
          new RoleBindingKey(principal("user" + i), "Operator", clusterA),
          new RoleBindingValue(Collections.emptySet()));
      authStore.consumer.addRecord(record);
    }
    TestUtils.waitForCondition(() -> authCache.rbacRoleBindings(clusterA).size() == 100,
        "Roles not assigned");
    for (int i = 0; i < 100; i++) {
      verifyRole("user" + i, "Operator");
    }
  }

  @Test
  public void testUsersDeletedIfNoLdap() throws Exception {
    createAuthStore();
    authCache = authStore.authCache();

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    KafkaPrincipal finance = new KafkaPrincipal("Group", "finance");
    KafkaPrincipal hr = new KafkaPrincipal("Group", "hr");
    authCache.put(new UserKey(alice), new UserValue(Collections.singleton(finance)));
    authCache.put(new UserKey(bob), new UserValue(Utils.mkSet(finance, hr)));
    assertEquals(Collections.singleton(finance), authCache.groups(alice));
    assertEquals(Utils.mkSet(finance, hr), authCache.groups(bob));

    authStore.startService(authStore.urls());
    TestUtils.waitForCondition(() -> authCache.groups(alice).isEmpty(), "User not deleted");
    TestUtils.waitForCondition(() -> authCache.groups(bob).isEmpty(), "User not deleted");
  }

  @Test
  public void testExternalStoreFailureBeforeStart() throws Exception {
    createAuthStoreWithLdap();
    miniKdcWithLdapService.stopLdap();
    CompletableFuture<Void> readerFuture = authStore.startReader().toCompletableFuture();
    startAuthService();

    TestUtils.waitForCondition(() -> {
      time.sleep(1000);
      return authCache.status(0) == MetadataStoreStatus.FAILED;
    }, "Ldap failure not propagated");
    assertFalse(readerFuture.isDone());
  }

  @Test
  public void testExternalStoreFailureAfterStart() throws Exception {
    createAuthStoreWithLdap();
    CompletableFuture<Void> readerFuture = authStore.startReader().toCompletableFuture();
    startAuthService();
    readerFuture.join();

    miniKdcWithLdapService.stopLdap();
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    TestUtils.waitForCondition(() -> {
      try {
        time.sleep(1000);
        authCache.groups(alice);
        return  false;
      } catch (MetadataStoreException e) {
        return true;
      }
    }, "Auth cache not failed");


    RbacTestUtils.verifyMetric("failure-start-seconds-ago", "LdapGroupManager", 1, 10);
    RbacTestUtils.verifyMetric("writer-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("record-send-rate", "KafkaAuthStore", 1, 10000);
    RbacTestUtils.verifyMetric("record-error-rate", "KafkaAuthStore", 0, 0);
  }

  @Test
  public void testAuthStoreStatus() throws Exception {
    AtomicBoolean failNextProduce = new AtomicBoolean();
    authStore = new MockAuthStore(rbacRoles, time, clusterA, 1, storeNodeId) {
      @Override
      protected Producer<AuthKey, AuthValue> createProducer(Map<String, Object> configs) {
        producer = new MockProducer<AuthKey, AuthValue>(cluster, false, null, null, null) {
          @Override
          public synchronized Future<RecordMetadata> send(ProducerRecord<AuthKey, AuthValue> record, Callback callback) {
            Future<RecordMetadata> future = super.send(record, callback);
            if (failNextProduce.getAndSet(false)) {
              producer.errorNext(new KafkaException("Test exception"));
            } else {
              producer.completeNext();
              consumer.addRecord(consumerRecord(record));
              return future;
            }
            return future;
          }
        };
        return producer;
      }
    };
    authStore.configure(Collections.singletonMap("confluent.metadata.bootstrap.servers", "localhost:9092,localhost:9093"));
    authStore.startReader();
    startAuthService();
    assertTrue(authStore.isMasterWriter());

    authWriter.addClusterRoleBinding(principal("user"), "Operator", clusterA)
        .toCompletableFuture().get(5, TimeUnit.SECONDS);
    RbacTestUtils.verifyMetric("record-send-rate", "KafkaAuthStore", 1, 1000);
    RbacTestUtils.verifyMetric("record-error-rate", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("writer-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    failNextProduce.set(true);
    assertThrows(ExecutionException.class, () -> authWriter.addClusterRoleBinding(principal("user2"), "Operator", clusterA)
        .toCompletableFuture().get(5, TimeUnit.SECONDS));
    RbacTestUtils.verifyMetric("record-error-rate", "KafkaAuthStore", 1, 1);

    failNextProduce.set(true);
    authWriter.writeExternalStatus(MetadataStoreStatus.INITIALIZED, "", 1);
    TestUtils.waitForCondition(() -> authStore.writerFailuresStartMs() != null, "Status update failure not propagated");
    RbacTestUtils.verifyMetric("record-error-rate", "KafkaAuthStore", 2, 5);

    authStore.consumer.addRecord(new ConsumerRecord<>(
        KafkaAuthStore.AUTH_TOPIC, 0, 10, new StatusKey(0), new StatusValue(MetadataStoreStatus.FAILED, 2, "Test failure")));
    TestUtils.waitForCondition(() -> authStore.remoteFailuresStartMs() != null, "Status update failure not propagated");

    authStore.consumer.addRecord(new ConsumerRecord<>(
        KafkaAuthStore.AUTH_TOPIC, 0, 11, new StatusKey(0), new StatusValue(MetadataStoreStatus.INITIALIZED, 3, "Test failure")));
    TestUtils.waitForCondition(() -> authStore.remoteFailuresStartMs() == null, "Status update failure not propagated");

  }

  private void createAuthStore() throws Exception {
    authStore = MockAuthStore.create(rbacRoles, time, Scope.intermediateScope("testOrg"), 1, storeNodeId);
  }

  private void createAuthStoreWithLdap() throws Exception {
    miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);

    authStore = new MockAuthStore(rbacRoles, time, Scope.intermediateScope("testOrg"), 1, storeNodeId);
    Map<String, Object> configs = new HashMap<>();
    configs.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 10));
    configs.put(LdapConfig.RETRY_BACKOFF_MS_PROP, "1");
    configs.put(LdapConfig.RETRY_BACKOFF_MAX_MS_PROP, "1");
    configs.put(LdapConfig.RETRY_TIMEOUT_MS_PROP, "1000");
    configs.put("confluent.metadata.bootstrap.servers", "localhost:9092,localhost:9093");
    authStore.configure(configs);
  }

  private void startAuthService() throws Exception {
    authStore.startService(authStore.urls());
    authWriter = authStore.writer();
    authCache = authStore.authCache();
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not elected");
    TestUtils.waitForCondition(() -> authStore.writer().ready(), "Writer not initialized");
  }

  private KafkaPrincipal principal(String userName) {
    return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName);
  }

  private void verifyRole(String userName, String role) {
    RoleBinding binding =
        new RoleBinding(principal(userName), role, clusterA, Collections.emptySet());
    assertTrue("Missing role for " + userName,
        authCache.rbacRoleBindings(clusterA).contains(binding));
  }
}
