// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.confluent.security.authorizer.provider.ProviderFailedException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

public class EmbeddedAuthorizerTest {

  private final EmbeddedAuthorizer authorizer = new EmbeddedAuthorizer();
  private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
  private final KafkaPrincipal group = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "groupA");
  private final ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic", PatternType.LITERAL);
  private final Scope scope = Scope.kafkaClusterScope("testScope");

  @After
  public void tearDown() {
    TestGroupProvider.reset();
    TestAccessRuleProvider.reset();
  }

  @Test
  public void testAccessRuleProvider() {
    configureAuthorizer("TEST", "NONE");
    verifyAccessRules(principal, principal);
  }

  @Test
  public void testGroupProvider() {
    configureAuthorizer("TEST", "TEST");
    TestGroupProvider.groups.put(principal, Collections.singleton(group));
    verifyAccessRules(principal, group);

    TestGroupProvider.groups.remove(principal);
    List<AuthorizeResult> result =
        authorizer.authorize(principal, "localhost", Arrays.asList(action("Write"), action("Read"), action("Alter")));
    assertEquals(Arrays.asList(AuthorizeResult.DENIED, AuthorizeResult.DENIED, AuthorizeResult.DENIED), result);
  }

  private void verifyAccessRules(KafkaPrincipal userPrincipal, KafkaPrincipal rulePrincipal) {

    Action write = action("Write");
    List<AuthorizeResult> result;
    result = authorizer.authorize(userPrincipal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(AuthorizeResult.DENIED), result);

    Action read = action("Read");
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(read, write));
    assertEquals(Arrays.asList(AuthorizeResult.DENIED, AuthorizeResult.DENIED), result);

    Set<AccessRule> topicRules = new HashSet<>();
    TestAccessRuleProvider.accessRules.put(topic, topicRules);
    topicRules.add(new AccessRule(rulePrincipal, PermissionType.ALLOW, "localhost", read.operation(), ""));

    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(read, write));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.DENIED), result);
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read));
    assertEquals(Arrays.asList(AuthorizeResult.DENIED, AuthorizeResult.ALLOWED), result);

    topicRules.add(new AccessRule(rulePrincipal, PermissionType.ALLOW, "localhost", write.operation(), ""));
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED), result);

    Action alter = action("Alter");
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read, alter));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED, AuthorizeResult.DENIED), result);
    TestAccessRuleProvider.superUsers.add(rulePrincipal);
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read, alter));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED), result);
  }

  @Test
  public void testAccessRuleProviderFailure() {
    configureAuthorizer("TEST", "NONE");
    TestAccessRuleProvider.exception = new ProviderFailedException("Provider failed");
    verifyProviderFailure(AuthorizeResult.AUTHORIZER_FAILED);
  }

  @Test
  public void testGroupProviderFailure() {
    configureAuthorizer("TEST", "TEST");
    TestGroupProvider.exception = new ProviderFailedException("Provider failed");
    verifyProviderFailure(AuthorizeResult.AUTHORIZER_FAILED);
  }

  @Test
  public void testUnexpectedException() {
    configureAuthorizer("TEST", "NONE");
    TestAccessRuleProvider.exception = new RuntimeException("Unknown failure");
    verifyProviderFailure(AuthorizeResult.UNKNOWN_ERROR);
  }

  private void verifyProviderFailure(AuthorizeResult expectedResult) {
    Action write = action("Write");
    List<AuthorizeResult> result;
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(expectedResult), result);

    TestAccessRuleProvider.superUsers.add(principal);
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(expectedResult), result);
  }

  @Test
  public void testInvalidScope() {
    configureAuthorizer("TEST", "NONE");

    Action write = new Action(Scope.kafkaClusterScope("someScope"), topic.resourceType(), topic.name(), new Operation("Write"));
    List<AuthorizeResult> result;
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(AuthorizeResult.UNKNOWN_SCOPE), result);

    TestAccessRuleProvider.superUsers.add(principal);
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(AuthorizeResult.UNKNOWN_SCOPE), result);
  }

  @Test
  public void testFutureOrTimeout() throws Exception {
    configureAuthorizer("TEST", "TEST");
    CompletableFuture<Void> future1 = new CompletableFuture<>();
    CompletableFuture<Void> future2 = authorizer.futureOrTimeout(future1, Duration.ofSeconds(60));

    TestUtils.waitForCondition(() -> threadCount("authorizer") == 1, "Timeout thread not created");
    try {
      future2.get(5, TimeUnit.MILLISECONDS);
      fail("Future completed before timeout or completion of stages");
    } catch (TimeoutException e) {
      // Expected exception
    }

    assertFalse(future1.isDone());
    assertFalse(future2.isDone());
    future1.complete(null);
    assertNull(future2.get(5, TimeUnit.SECONDS));
    TestUtils.waitForCondition(() -> threadCount("authorizer") == 0, "Timeout thread not deleted");

    CompletableFuture<Void> future3 = new CompletableFuture<>();
    CompletableFuture<Void> future4 = authorizer.futureOrTimeout(future3, Duration.ofMillis(5));
    try {
      future4.get();
      fail("Future completed before timeout or completion of stages");
    } catch (ExecutionException e) {
      assertEquals(org.apache.kafka.common.errors.TimeoutException.class, e.getCause().getClass());
      assertFalse(future3.isDone());
    }
    TestUtils.waitForCondition(() -> threadCount("authorizer") == 0, "Timeout thread not deleted");
  }

  private long threadCount(String prefix) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(prefix))
        .count();
  }

  private void configureAuthorizer(String accessRuleProvider, String groupProvider) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, accessRuleProvider);
    props.put(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP, groupProvider);
    authorizer.configure(props);
  }

  private Action action(String operation) {
    return new Action(scope, topic.resourceType(), topic.name(), new Operation(operation));
  }

}
