/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import static io.confluent.events.EventLoggerConfig.KAFKA_EXPORTER_PREFIX;
import static io.confluent.security.audit.AuditLogConfig.ROUTER_CONFIG;
import static io.confluent.security.test.utils.User.scramUser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.events.ProtobufEvent;
import io.confluent.events.cloudevents.extensions.RouteExtension;
import io.confluent.events.exporter.Exporter;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

// Test routing
public class ProviderRoutingTest {

  // Same as the AuditLogRouterTest.
  private String json = "{\"destinations\":{\"bootstrap_servers\":[\"host1:port\",\"host2:port\"],\"topics\":{\"_confluent-audit-log_success\":{\"retention_ms\":2592000000},\"_confluent-audit-log_failure\":{\"retention_ms\":2592000000},\"_confluent-audit-log_ksql\":{\"retention_ms\":2592000000},\"_confluent-audit-log_connect_success\":{\"retention_ms\":2592000000},\"_confluent-audit-log_connect_failure\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_produce_allowed\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_produce_denied\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_consume_allowed\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_consume_denied\":{\"retention_ms\":15552000000},\"_confluent-audit-log_accounting\":{\"retention_ms\":15552000000},\"_confluent-audit-log_cluster\":{\"retention_ms\":15552000000}}},\"default_topics\":{\"allowed\":\"_confluent-audit-log_success\",\"denied\":\"_confluent-audit-log_failure\"},\"excluded_principals\":[\"User:Alice\",\"User:service_account_id\"],\"routes\":{\"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\":{\"authorize\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_ksql\"}},\"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=*\":{\"authorize\":{\"allowed\":\"_confluent-audit-log_connect_success\",\"denied\":\"_confluent-audit-log_connect_failure\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks\":{\"produce\":{\"allowed\":\"_confluent-audit-log_clicks_produce_allowed\",\"denied\":\"_confluent-audit-log_clicks_produce_denied\"},\"consume\":{\"allowed\":\"_confluent-audit-log_clicks_consume_allowed\",\"denied\":\"_confluent-audit-log_clicks_consume_denied\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-*\":{\"produce\":{\"allowed\":null,\"denied\":\"_confluent-audit-log_accounting\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=*\":{\"produce\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"consume\":{\"denied\":\"_confluent-audit-log_cluster\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA\":{\"interbroker\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"other\":{\"denied\":\"_confluent-audit-log_cluster\"}},\"crn://mds1.example.com/kafka=*\":{\"interbroker\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"other\":{\"denied\":\"_confluent-audit-log_cluster\"}}},\"metadata\":{\"resource_version\":\"f109371d0a856a40a2a96cca98f90ec2\",\"updated_at\":\"2019-08-21T18:31:47+00:00\"}}";

  private Map<String, Object> configs = Utils.mkMap(
      Utils.mkEntry(
          AuditLogConfig.BOOTSTRAP_SERVERS_CONFIG,
          "localhost:9092"),
      Utils.mkEntry(AuditLogConfig.TOPIC_CREATE_CONFIG, "false"),
      Utils.mkEntry(CrnAuthorityConfig.AUTHORITY_NAME_PROP, "mds.example.com"),
      Utils.mkEntry(
          ROUTER_CONFIG,
          AuditLogRouterJsonConfigUtils.defaultConfig("localhost:9092", "", "")));


  private ConfluentAuditLogProvider providerWithMockExporter(String clusterId,
      Map<String, String> mkMap) throws Exception {
    ConfluentAuditLogProvider provider = new ConfluentAuditLogProvider();
    configs.put(ROUTER_CONFIG, json);
    configs.put(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, MockExporter.class.getName());
    configs.putAll(mkMap);
    provider.configure(configs);
    provider.onUpdate(new ClusterResource(clusterId));
    CompletableFuture<Void> startFuture = provider.start(configs).toCompletableFuture();
    startFuture.get(10_000, TimeUnit.MILLISECONDS);
    return provider;
  }

  @Test
  public void testEventLoggedOnDefaultTopic() throws Throwable {
    // This is the default topic configured in the config, not EventLoggerConfig.DEFAULT_TOPIC_CONFIG
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_success";

    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action write = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, write, AuthorizeResult.ALLOWED, policy);

    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();
    assertEquals(1, ma.events.size());

    CloudEvent<AttributesImpl, AuditLogEntry> event = ma.events.get(0);

    // Attributes
    assertNotNull(event.getAttributes().getId());
    assertTrue(event.getAttributes().getTime().isPresent());
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=testTopic",
        event.getAttributes().getSubject().get());
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA",
        event.getAttributes().getSource().toString());
    assertEquals(ProtobufEvent.APPLICATION_JSON, event.getAttributes().getDatacontenttype().get());
    assertEquals("0.3", event.getAttributes().getSpecversion());
    assertEquals("io.confluent.kafka.server/authorization", event.getAttributes().getType());

    // Data
    assertTrue(event.getData().isPresent());
    AuditLogEntry ae = event.getData().get();
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA", ae.getServiceName());
    assertEquals("crn://mds.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=testTopic",
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

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("Produce"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    assertEquals(1, ma.events.size());

    CloudEvent<?, AuditLogEntry> event = ma.events.get(0);
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

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "payroll",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.PRODUCE, (short) 1, "", 1), "", InetAddress.getLoopbackAddress(),
        principal, ListenerName.normalised("EXTERNAL"), SecurityProtocol.SASL_SSL,
        RequestContext.KAFKA);
    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("Produce"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testEventExcludedUser() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Route"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.METADATA, (short) 1, "", 1), "", InetAddress.getLoopbackAddress(),
        principal, ListenerName.normalised("EXTERNAL"), SecurityProtocol.SASL_SSL,
        RequestContext.KAFKA);

    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("Write"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);

    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testEventLogPrinicipalExcluded() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap(
            Utils.mkEntry(AuditLogConfig.AUDIT_LOG_PRINCIPAL_CONFIG, "User:FOO")));
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "FOO");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Route"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.METADATA, (short) 1, "", 1), "", InetAddress.getLoopbackAddress(),
        principal, ListenerName.normalised("EXTERNAL"), SecurityProtocol.SASL_SSL,
        RequestContext.KAFKA);

    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("Write"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);

    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testDefaultEventLogPrinicipalExcluded() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    KafkaPrincipal principal = SecurityUtils
        .parseKafkaPrincipal(AuditLogConfig.DEFAULT_AUDIT_LOG_PRINCIPAL_CONFIG);
    ResourcePattern topic = new ResourcePattern(new ResourceType("Route"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.METADATA, (short) 1, "", 1), "", InetAddress.getLoopbackAddress(),
        principal, ListenerName.normalised("EXTERNAL"), SecurityProtocol.SASL_SSL,
        RequestContext.KAFKA);

    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("Write"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);

    assertEquals(0, ma.events.size());

    provider.close();
  }

  @Test
  public void testSendLogOnlyWhenRouteIsReady() throws Throwable {
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA",
        Utils.mkMap());
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "FOO");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Route"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    ma.routeReady = false;
    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);

    assertEquals(0, ma.events.size());

    ma.routeReady = true;
    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);
    assertEquals(1, ma.events.size());

    provider.close();
  }

  @Test
  public void testReconfigure() throws Throwable {
    // Make sure defaults work.
    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig(
            "foo:9092",
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC))
    );
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA", config);

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    assertEquals(1, ma.events.size());

    CloudEvent<AttributesImpl, AuditLogEntry> event = ma.events.get(0);
    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
    RouteExtension re = (RouteExtension) event.getExtensions()
        .get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(AuditLogRouterJsonConfig.DEFAULT_TOPIC, re.getRoute());

    // Change routes.
    String newAllowedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "__allowed_new";
    String newDeniedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "__denied_new";

    config = Utils.mkMap(
        Utils.mkEntry(ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                "foo-recongigured:9200",
                newAllowedTopic,
                newDeniedTopic)),
        Utils.mkEntry(KAFKA_EXPORTER_PREFIX + SaslConfigs.SASL_JAAS_CONFIG,
            scramUser("eventLogReader", "foo").jaasConfig),
        Utils.mkEntry(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, MockExporter.class.getName())
    );
    provider.reconfigure(config);

    principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");
    topic = new ResourcePattern(new ResourceType("Topic"), "testTopic", PatternType.LITERAL);

    requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER, principal);

    ma = (MockExporter) provider.getEventLogger().eventExporter();
    ma.events.clear();
    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);

    assertEquals(1, ma.events.size());

    event = ma.events.get(0);
    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));

    // Make sure the event is routed to the new topic.
    re = (RouteExtension) event.getExtensions().get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(newAllowedTopic, re.getRoute());

    ma.events.clear();
    provider.logAuthorization(requestContext, create, AuthorizeResult.DENIED, policy);

    assertEquals(1, ma.events.size());

    event = ma.events.get(0);
    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));

    // Make sure the event is routed to the new topic.
    re = (RouteExtension) event.getExtensions().get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(newDeniedTopic, re.getRoute());
    provider.close();
  }

  @Test
  public void testReconfigureEmptyTopics() throws Throwable {
    // Make sure defaults work.
    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig(
            "foo:9092",
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC))
    );
    ConfluentAuditLogProvider provider = providerWithMockExporter("63REM3VWREiYtMuVxZeplA", config);

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");
    ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "testTopic",
        PatternType.LITERAL);

    RequestContext requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    Action create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
        principal);

    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);
    MockExporter ma = (MockExporter) provider.getEventLogger().eventExporter();

    assertEquals(1, ma.events.size());

    CloudEvent<?, AuditLogEntry> event = ma.events.get(0);
    assertTrue(event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
    RouteExtension re = (RouteExtension) event.getExtensions()
        .get(RouteExtension.Format.IN_MEMORY_KEY);
    assertEquals(AuditLogRouterJsonConfig.DEFAULT_TOPIC, re.getRoute());

    // Change routes.
    config = Utils.mkMap(
        Utils.mkEntry(ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                "foo-recongigured:9200",
                "",
                "")),
        Utils.mkEntry(KAFKA_EXPORTER_PREFIX + SaslConfigs.SASL_JAAS_CONFIG,
            scramUser("eventLogReader", "foo").jaasConfig),
        Utils.mkEntry(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, MockExporter.class.getName())
    );
    provider.reconfigure(config);

    principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");
    topic = new ResourcePattern(new ResourceType("Topic"), "testTopic", PatternType.LITERAL);

    requestContext = new MockRequestContext(
        new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 1, "", 1), "",
        InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
        SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
    create = new Action(provider.getScope(), topic.resourceType(), topic.name(),
        new Operation("CreateTopics"));
    policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER, principal);

    ma = (MockExporter) provider.getEventLogger().eventExporter();
    ma.events.clear();
    provider.logAuthorization(requestContext, create, AuthorizeResult.ALLOWED, policy);

    // Make sure the event is not routed
    assertEquals(0, ma.events.size());

    ma.events.clear();
    provider.logAuthorization(requestContext, create, AuthorizeResult.DENIED, policy);
    // Make sure the event is not routed
    assertEquals(0, ma.events.size());

  }


  private class MockRequestContext implements RequestContext {

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

  public static class MockExporter implements Exporter {

    public RuntimeException configureException;
    public boolean routeReady = true;
    public List<CloudEvent> events = new ArrayList<>();

    public MockExporter() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
      if (configureException != null) {
        throw configureException;
      }
    }

    @Override
    public void append(CloudEvent event) throws RuntimeException {
      events.add(event);
    }

    @Override
    public boolean routeReady(CloudEvent event) {
      return routeReady;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
      return Collections.emptySet();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
    }

    @Override
    public void close() throws Exception {
    }
  }
}