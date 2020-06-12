/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import static io.confluent.security.audit.router.AuditLogRouterJsonConfig.DEFAULT_TOPIC;
import static io.confluent.security.audit.router.AuditLogRouterJsonConfig.TOPIC_PREFIX;
import static io.confluent.security.audit.telemetry.exporter.NonBlockingKafkaExporterConfig.KAFKA_EXPORTER_PREFIX;
import static io.confluent.security.test.utils.User.scramUser;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.ENABLE_AUTHENTICATION_AUDIT_LOGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.kafka.server.plugins.auth.PlainSaslServer;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.audit.provider.ConfluentAuditLogProvider.AuditLogMetrics;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationEvent;
import org.apache.kafka.server.audit.DefaultAuthenticationEvent;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

@SuppressWarnings("unchecked")
// Test routing
public class ProviderRoutingTest {
  // This test uses sample-audit-log-routing.json file content as the audit log routing config (See AuditLogRouterTest).

  private final Map<String, Object> configs = Utils.mkMap(
      Utils.mkEntry(
          AuditLogConfig.BOOTSTRAP_SERVERS_CONFIG,
          "localhost:9092"),
      Utils.mkEntry(AuditLogConfig.TOPIC_CREATE_CONFIG, "false"),
      Utils.mkEntry(CRN_AUTHORITY_NAME_CONFIG, "mds1.example.com"),
      Utils.mkEntry(
          AUDIT_EVENT_ROUTER_CONFIG,
          AuditLogRouterJsonConfigUtils.defaultConfig("localhost:9092", "", "")));

  private ConfluentAuditLogProvider providerWithMockExporter(String clusterId,
      Map<String, String> configOverrides) throws Exception {
    return providerWithMockExporter(clusterId, configOverrides, true);
  }

  private ConfluentAuditLogProvider providerWithMockExporter(String clusterId,
                                                             Map<String, String> configOverrides,
                                                             Boolean startProvider) throws Exception {
    ConfluentAuditLogProvider provider = new ConfluentAuditLogProvider();
    configs.put(AUDIT_EVENT_ROUTER_CONFIG, routerConfig());
    configs.put(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, MockExporter.class.getName());
    configs.putAll(configOverrides);
    provider.configure(configs);
    provider.onUpdate(new ClusterResource(clusterId));
    provider.setMetrics(new Metrics());
    if (startProvider) {
      CompletableFuture<Void> startFuture = provider
              .start(configs).toCompletableFuture();
      startFuture.get(10_000, TimeUnit.MILLISECONDS);
    }

    return provider;
  }

  private String routerConfig() throws IOException {
    final String path = ProviderRoutingTest.class.getResource("/sample-audit-log-routing.json").getFile();
    return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
  }

  @Test
  public void testEventLoggedOnDefaultTopic() throws Throwable {
    // This is the default topic configured in the config, not EventLoggerConfig.DEFAULT_TOPIC_CONFIG
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_success";

    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action write = new Action(clusterScope, topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logEvent(
        new ConfluentAuthorizationEvent(clusterScope, requestContext, write, AuthorizeResult.ALLOWED,
            policy));

    MockExporter<AuditLogEntry> ma = (MockExporter) provider.getEventLogger().eventExporter();
    assertEquals(1, ma.events.size());

    CloudEvent<AttributesImpl, AuditLogEntry> event = ma.events.get(0);

    // Attributes
    assertNotNull(event.getAttributes().getId());
    assertTrue(event.getAttributes().getTime().isPresent());
    assertEquals("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=testTopic",
        event.getAttributes().getSubject().get());
    assertEquals("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
        event.getAttributes().getSource().toString());
    assertEquals(Protobuf.APPLICATION_JSON, event.getAttributes().getDatacontenttype().get());
    assertEquals("1.0", event.getAttributes().getSpecversion());
    assertEquals("io.confluent.kafka.server/authorization", event.getAttributes().getType());

    // Data
    assertTrue(event.getData().isPresent());
    AuditLogEntry ae = event.getData().get();
    assertEquals("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA", ae.getServiceName());
    assertEquals("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=testTopic",
        ae.getResourceName());
    assertEquals("kafka.CreateTopics", ae.getMethodName());
    assertEquals("User:user1", ae.getAuthenticationInfo().getPrincipal());
    assertTrue(ae.getAuthorizationInfo().getGranted());
    assertEquals("CreateTopics", ae.getAuthorizationInfo().getOperation());
    assertEquals("testTopic", ae.getAuthorizationInfo().getResourceName());
    assertEquals("Topic", ae.getAuthorizationInfo().getResourceType());
    assertEquals("LITERAL", ae.getAuthorizationInfo().getPatternType());

    // Extensions
    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
    RouteExtension re = (RouteExtension) event.getExtensions()
        .get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(expectedTopic, re.getRoute());

    provider.close();

  }

  @Test
  public void testEventLoggedOnRoutedTopic() throws Throwable {
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_success";

    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(clusterScope, topic.resourceType(), topic.name(),
        new Operation("Produce"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider
        .logEvent(
            new ConfluentAuthorizationEvent(clusterScope, requestContext, create, AuthorizeResult.ALLOWED,
                policy));
    MockExporter<AuditLogEntry> ma = (MockExporter) provider.getEventLogger().eventExporter();

    assertEquals(1, ma.events.size());

    CloudEvent<AttributesImpl, AuditLogEntry> event = ma.events.get(0);
    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
    RouteExtension re = (RouteExtension) event.getExtensions()
        .get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(expectedTopic, re.getRoute());

    provider.close();
  }

  @Test
  public void testEventSuppressedTopic() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "payroll",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.PRODUCE, (short) 1, "", 1), "", InetAddress.getLoopbackAddress(),
        principal, ListenerName.normalised("EXTERNAL"), SecurityProtocol.SASL_SSL,
        RequestContext.KAFKA);
    Action create = new Action(clusterScope, topic.resourceType(), topic.name(),
        new Operation("Produce"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider
        .logEvent(
            new ConfluentAuthorizationEvent(clusterScope, requestContext, create, AuthorizeResult.ALLOWED,
                policy));
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testEventExcludedUser() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Route"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.METADATA, (short) 1, "", 1), "", InetAddress.getLoopbackAddress(),
        principal, ListenerName.normalised("EXTERNAL"), SecurityProtocol.SASL_SSL,
        RequestContext.KAFKA);

    Action create = new Action(clusterScope, topic.resourceType(), topic.name(),
        new Operation("Write"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider
        .logEvent(
            new ConfluentAuthorizationEvent(clusterScope, requestContext, create, AuthorizeResult.ALLOWED,
                policy));

    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testProduceToSameTopicExcluded() throws Throwable {
    // we should not audit log produce events to the same topic
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap(
            Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG,
                AuditLogRouterJsonConfigUtils.defaultConfigProduceConsumeInterbroker(
                    "host1:port",
                    "mds1.example.com",
                    DEFAULT_TOPIC,
                    DEFAULT_TOPIC,
                    Collections.emptyList()))
        )
    );
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal("User:kafka");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"),
        DEFAULT_TOPIC,
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.PRODUCE, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(clusterScope, topic.resourceType(), topic.name(),
        new Operation("Produce"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider
        .logEvent(
            new ConfluentAuthorizationEvent(clusterScope, requestContext, create, AuthorizeResult.ALLOWED,
                policy));

    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testSendLogOnlyWhenRouteIsReady() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "FOO");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Route"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(clusterScope, topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    ma.routeReady = false;
    provider
        .logEvent(
            new ConfluentAuthorizationEvent(clusterScope, requestContext, create, AuthorizeResult.ALLOWED,
                policy));

    assertEquals(0, ma.events.size());

    ma.routeReady = true;
    provider
        .logEvent(
            new ConfluentAuthorizationEvent(clusterScope, requestContext, create, AuthorizeResult.ALLOWED,
                policy));
    assertEquals(1, ma.events.size());

    provider.close();
  }

  @Test
  public void testReconfigure() throws Throwable {
    // Make sure defaults work.
    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig(
            "foo:9092",
            DEFAULT_TOPIC,
            DEFAULT_TOPIC))
    );
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA", config);
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");

    checkTestMessage(provider, clusterScope, AuthorizeResult.ALLOWED, DEFAULT_TOPIC);
    // Change routes.
    String newAllowedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "__allowed_new";
    String newDeniedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "__denied_new";

    config = Utils.mkMap(
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                "foo-recongigured:9200",
                newAllowedTopic,
                newDeniedTopic)),
        Utils.mkEntry(KAFKA_EXPORTER_PREFIX + SaslConfigs.SASL_JAAS_CONFIG,
            scramUser("eventLogReader", "foo").jaasConfig),
        Utils.mkEntry(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, MockExporter.class.getName())
    );
    provider.reconfigure(config);

    // Make sure the events are routed to the new topics.
    checkTestMessage(provider, clusterScope, AuthorizeResult.ALLOWED, newAllowedTopic);
    checkTestMessage(provider, clusterScope, AuthorizeResult.DENIED, newDeniedTopic);

    // Configure this with an empty config, which resets to the defaults
    config = Utils.mkMap();
    provider.reconfigure(config);

    // Make sure the events are routed to the new topics.
    checkTestMessage(provider, clusterScope, AuthorizeResult.ALLOWED, DEFAULT_TOPIC);
    checkTestMessage(provider, clusterScope, AuthorizeResult.DENIED, DEFAULT_TOPIC);

    provider.close();
  }

  @Test
  public void testReconfigureEmptyTopics() throws Throwable {
    // Make sure defaults work.
    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig(
            "foo:9092",
            TOPIC_PREFIX + "_allowed",
            TOPIC_PREFIX + "_denied"))
    );
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA", config);
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");

    checkTestMessage(provider, clusterScope, AuthorizeResult.ALLOWED, TOPIC_PREFIX + "_allowed");
    checkTestMessage(provider, clusterScope, AuthorizeResult.DENIED, TOPIC_PREFIX + "_denied");

    // Change routes.
    config = Utils.mkMap(
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                "foo-recongigured:9200",
                "",
                "")),
        Utils.mkEntry(KAFKA_EXPORTER_PREFIX + SaslConfigs.SASL_JAAS_CONFIG,
            scramUser("eventLogReader", "foo").jaasConfig),
        Utils.mkEntry(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, MockExporter.class.getName())
    );
    provider.reconfigure(config);

    // reconfigured to send messages nowhere
    checkTestMessage(provider, clusterScope, AuthorizeResult.ALLOWED, "");
    checkTestMessage(provider, clusterScope, AuthorizeResult.DENIED, "");

    provider.close();
  }

  @Test
  public void testReconfigureEmpty() throws Throwable {
    // Make sure defaults work.
    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig(
            "foo:9092",
            TOPIC_PREFIX + "_allowed",
            TOPIC_PREFIX + "_denied"))
    );
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA", config);
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");

    checkTestMessage(provider, clusterScope, AuthorizeResult.ALLOWED, TOPIC_PREFIX + "_allowed");

    // This sets the routes to the default! Which is different from the originally-configured value
    config = Utils.mkMap(
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG, "")
    );
    provider.reconfigure(config);

    checkTestMessage(provider, clusterScope, AuthorizeResult.ALLOWED, DEFAULT_TOPIC);

    provider.close();
  }

  @Test
  public void testAuditLogMetricRate() throws Exception {
    MockTime time = new MockTime();
    ConfluentAuthorizationEvent authEvent = createConfluentAuthorizationEvent(true, true);
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA", Utils.mkMap());
    // reset Metrics
    provider.setupMetrics(time);
    for (int i = 0; i < 20; i++) {
      provider.logEvent(authEvent);
    }
    provider.metricsTime().sleep(20000);
    assertTrue(TestUtils.getMetricValue(provider.metrics(), AuditLogMetrics.AUDIT_LOG_RATE_MINUTE) >= 20);
  }

  @Test
  public void testAuditLogMetricsRouteNotReady() throws Exception {
    MockTime time = new MockTime();
    ConfluentAuthorizationEvent authEvent = createConfluentAuthorizationEvent(true, true);
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA", Utils.mkMap());
    MockExporter export = (MockExporter) provider.getEventLogger().eventExporter();
    export.setRouteReady(false);
    // reset Metrics
    provider.setupMetrics(time);
    for (int i = 0; i < 20; i++) {
      provider.logEvent(authEvent);
    }
    provider.metricsTime().sleep(20000);
    assertTrue(TestUtils.getMetricValue(provider.metrics(), AuditLogMetrics.AUDIT_LOG_FALLBACK_RATE_MINUTE) >= 20);
  }

  @Test
  public void testAuditLogMetricsEventLoggerNotReady() throws Exception {
    MockTime time = new MockTime();
    ConfluentAuthorizationEvent authEvent = createConfluentAuthorizationEvent(true, true);
    ConfluentAuditLogProvider provider =
            providerWithMockExporter("63REM3VWREiYtMuVxZeplA", Utils.mkMap(), false);
    // reset Metrics
    provider.setupMetrics(time);
    for (int i = 0; i < 20; i++) {
      provider.logEvent(authEvent);
    }
    provider.metricsTime().sleep(20000);
    assertTrue(TestUtils.getMetricValue(provider.metrics(), AuditLogMetrics.AUDIT_LOG_FALLBACK_RATE_MINUTE) >= 20);
  }

  @Test
  public void testAuditLogFallbackRate() throws Exception {
    MockTime time = new MockTime();
    ConfluentAuthorizationEvent authEvent = createConfluentAuthorizationEvent(false, false);
    ConfluentAuditLogProvider provider =
            providerWithMockExporter("63REM3VWREiYtMuVxZeplA", Utils.mkMap());
    // reset Metrics
    provider.setupMetrics(time);
    for (int i = 0; i < 20; i++) {
      provider.logEvent(authEvent);
    }
    provider.metricsTime().sleep(20000);
    assertTrue(TestUtils.getMetricValue(provider.metrics(), AuditLogMetrics.AUDIT_LOG_FALLBACK_RATE_MINUTE) >= 20);
  }

  public void checkTestMessage(ConfluentAuditLogProvider provider, Scope clusterScope, AuthorizeResult result, String expectedTopic) {
    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(clusterScope, topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    MockExporter<AuditLogEntry> ma = (MockExporter) provider.getEventLogger().eventExporter();
    ma.events.clear();
    provider
        .logEvent(
            new ConfluentAuthorizationEvent(clusterScope, requestContext, create, result,
                policy));

    if (expectedTopic != null && !expectedTopic.isEmpty()) {
      assertEquals(1, ma.events.size());

      CloudEvent<?, AuditLogEntry> event = ma.events.get(0);
      assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
      RouteExtension re = (RouteExtension) event.getExtensions()
          .get(RouteExtension.Format.IN_MEMORY_KEY);
      assertEquals(expectedTopic, re.getRoute());
    } else {
      // Make sure the event is not routed
      assertEquals(0, ma.events.size());
    }
  }

  private ConfluentAuthorizationEvent createConfluentAuthorizationEvent(boolean logIfAllowed, boolean logIfDenied) {
    Scope clusterScope = Scope.kafkaClusterScope("63REM3VWREiYtMuVxZeplA");

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
    ResourcePattern topicPattern = new ResourcePattern(new ResourceType("Topic"), "testTopic",
            PatternType.LITERAL);
    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action write = new Action(clusterScope, topicPattern, new Operation("CreateTopics"), 1, logIfAllowed, logIfDenied);
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
            principal);

    return new ConfluentAuthorizationEvent(clusterScope, requestContext, write, AuthorizeResult.ALLOWED, policy);
  }

  @Test
  public void testAuthenticationEventLoggedOnDefaultTopic() throws Throwable {
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_success";

    ConfluentAuditLogProvider provider = providerWithMockExporter("Test_Cluster",
        Utils.mkMap(
            Utils.mkEntry(ENABLE_AUTHENTICATION_AUDIT_LOGS, "true")
        ));
    AuthenticationEvent authenticationEvent = createAuthenticationEvent(AuditEventStatus.SUCCESS, "user1");

    provider.logEvent(authenticationEvent);

    MockExporter<AuditLogEntry> ma = (MockExporter) provider.getEventLogger().eventExporter();
    assertEquals(1, ma.events.size());

    CloudEvent<AttributesImpl, AuditLogEntry> event = ma.events.get(0);

    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
    RouteExtension re = (RouteExtension) event.getExtensions()
        .get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(expectedTopic, re.getRoute());

    provider.close();
  }

  @Test
  public void testAuthenticationEventLoggedOnRoutedTopic() throws Throwable {
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_authentication_failure";

    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap(
            Utils.mkEntry(ENABLE_AUTHENTICATION_AUDIT_LOGS, "true")
        ));
    AuthenticationEvent authenticationEvent = createAuthenticationEvent(AuditEventStatus.UNAUTHENTICATED, "user1");

    provider.logEvent(authenticationEvent);

    MockExporter<AuditLogEntry> ma = (MockExporter) provider.getEventLogger().eventExporter();
    assertEquals(1, ma.events.size());

    CloudEvent<AttributesImpl, AuditLogEntry> event = ma.events.get(0);

    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
    RouteExtension re = (RouteExtension) event.getExtensions()
        .get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(expectedTopic, re.getRoute());

    provider.close();
  }

  @Test
  public void testAuthenticationEventExcludedUser() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap(
            Utils.mkEntry(ENABLE_AUTHENTICATION_AUDIT_LOGS, "true")
        ));

    AuthenticationEvent authenticationEvent = createAuthenticationEvent(AuditEventStatus.UNAUTHENTICATED, "Alice");

    provider.logEvent(authenticationEvent);

    MockExporter<AuditLogEntry> ma = (MockExporter) provider.getEventLogger().eventExporter();
    assertEquals(0, ma.events.size());

    provider.close();
  }

  private DefaultAuthenticationEvent createAuthenticationEvent(AuditEventStatus auditEventStatus, String userName) {
    PlainSaslServer server = mock(PlainSaslServer.class);
    when(server.getMechanismName()).thenReturn(PlainSaslServer.PLAIN_MECHANISM);
    when(server.userIdentifier()).thenReturn("APIKEY123");

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName);

    AuthenticationContext authenticationContext = new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL,
        InetAddress.getLoopbackAddress(), SecurityProtocol.SASL_SSL.name());
    return new DefaultAuthenticationEvent(principal, authenticationContext, auditEventStatus);
  }

  private static class MockRequestContext implements RequestContext {

    public final RequestHeader header;
    public final String connectionId;
    public final InetAddress clientAddress;
    public final KafkaPrincipal principal;
    public final ListenerName listenerName;
    public final SecurityProtocol securityProtocol;
    public final String requestSource;

    public MockRequestContext(RequestHeader header, String connectionId, InetAddress clientAddress,
        KafkaPrincipal principal, ListenerName listenerName, SecurityProtocol securityProtocol,
        String requestSource) {
      this.header = header;
      this.connectionId = connectionId;
      this.clientAddress = clientAddress;
      this.principal = principal;
      this.listenerName = listenerName;
      this.securityProtocol = securityProtocol;
      this.requestSource = requestSource;
    }

    @Override
    public String listenerName() {
      return listenerName.value();
    }

    @Override
    public SecurityProtocol securityProtocol() {
      return securityProtocol;
    }

    @Override
    public KafkaPrincipal principal() {
      return principal;
    }

    @Override
    public InetAddress clientAddress() {
      return clientAddress;
    }

    @Override
    public int requestType() {
      return header.apiKey().id;
    }

    @Override
    public int requestVersion() {
      return header.apiVersion();
    }

    @Override
    public String clientId() {
      return header.clientId();
    }

    @Override
    public int correlationId() {
      return header.correlationId();
    }

    @Override
    public String requestSource() {
      return requestSource;
    }
  }
}
