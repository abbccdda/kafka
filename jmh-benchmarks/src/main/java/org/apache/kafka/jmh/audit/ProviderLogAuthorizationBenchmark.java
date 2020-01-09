package org.apache.kafka.jmh.audit;

import static io.confluent.crn.CrnAuthorityConfig.AUTHORITY_NAME_PROP;
import static io.confluent.events.cloudevents.kafka.Marshallers.structuredProto;
import static io.confluent.security.audit.AuditLogConfig.ROUTER_CONFIG;

import io.cloudevents.CloudEvent;
import io.cloudevents.format.Wire;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.events.cloudevents.extensions.RouteExtension;
import io.confluent.events.exporter.Exporter;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.provider.ConfluentAuditLogProvider;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for tuning Audit Logging performance. This calls the logAuthorization method with a
 * number of different configurations: - not logging any messages (with the defaults set to "") This
 * measures the cost of creating an AuditLogEntry and using the router to determine its disposition
 * - Logging only Create authorizations This simulates the default "Management only" auditing, with
 * 1/33 of the authorizations being "Management" operations - Logging Create authorizations and 1/97
 * of Produce authorizations Very selective Produce logging - Logging Create and 11/97 of Produce
 * authorizations Not selective Produce logging - Logging Create and 97/97 of Produce authorizations
 * Comprehensive Produce logging - Logging Everything Comprehensive Produce and consume logging
 */

@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProviderLogAuthorizationBenchmark {

  private static final String CLUSTER_ID = "63REM3VWREiYtMuVxZeplA";
  private static final int DISTINCT_KEYS = 10_000;
  private static final int USERS = 101;
  private static final int TOPICS = 97;
  private static final String[] ACTIONS;
  private static final ApiKeys[] API_KEYS;

  static {
    ArrayList<String> actions = new ArrayList<>();
    ArrayList<ApiKeys> apiKeys = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      actions.add("Produce");
      apiKeys.add(ApiKeys.PRODUCE);
      actions.add("FetchConsumer");
      apiKeys.add(ApiKeys.FETCH);
    }
    actions.add("CreateTopics");
    apiKeys.add(ApiKeys.CREATE_TOPICS);
    ACTIONS = actions.toArray(new String[0]);
    API_KEYS = apiKeys.toArray(new ApiKeys[0]);
  }


  private long counter = 0;

  private final LogAuthorizationArguments[] args = new LogAuthorizationArguments[DISTINCT_KEYS];
  private ConfluentAuditLogProvider provider;

  private static ConfluentAuditLogProvider noneProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"\",\n"
              + "        \"denied\": \"\"\n"
              + "    }\n"
              + "}");

  private static ConfluentAuditLogProvider createProduceOneLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=topic0\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  private static ConfluentAuditLogProvider createProduceSomeLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=topic1*\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  private static ConfluentAuditLogProvider createProduceAllLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=*\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  private static ConfluentAuditLogProvider everythingLogProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"routes\": {\n"
              + "        \"crn://mds.example.com/kafka=*/topic=*\": {\n"
              + "            \"produce\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            },\n"
              + "            \"consume\": {\n"
              + "                \"allowed\": \"confluent-audit-log-events-produce\",\n"
              + "                \"denied\": \"confluent-audit-log-events-produce\"\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-produce\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  private static ConfluentAuditLogProvider createProvider =
      providerWithCountExporter(
          "{\n"
              + "    \"destinations\": {\n"
              + "        \"topics\": {\n"
              + "            \"confluent-audit-log-events-allowed\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            },\n"
              + "            \"confluent-audit-log-events-denied\": {\n"
              + "                \"retention_ms\": 7776000000\n"
              + "            }\n"
              + "        }\n"
              + "    },\n"
              + "    \"default_topics\": {\n"
              + "        \"allowed\": \"confluent-audit-log-events-allowed\",\n"
              + "        \"denied\": \"confluent-audit-log-events-denied\"\n"
              + "    }\n"
              + "}");

  private static class LogAuthorizationArguments {

    public final Scope sourceScope;
    public final RequestContext requestContext;
    public final Action action;
    public final AuthorizeResult authorizeResult;
    public final AuthorizePolicy authorizePolicy;

    private LogAuthorizationArguments(
        Scope sourceScope,
        RequestContext requestContext,
        Action action,
        AuthorizeResult authorizeResult,
        AuthorizePolicy authorizePolicy) {
      this.sourceScope = sourceScope;
      this.requestContext = requestContext;
      this.action = action;
      this.authorizeResult = authorizeResult;
      this.authorizePolicy = authorizePolicy;
    }
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

  public static class CountExporter implements Exporter {

    public RuntimeException configureException;
    public boolean routeReady = true;
    public ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();
    private EventStep<AttributesImpl, ? extends Object, byte[], byte[]> builder;

    public CountExporter() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
      if (configureException != null) {
        throw configureException;
      }
      this.builder = structuredProto();
    }

    private String route(CloudEvent event) {
      if (event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY)) {
        RouteExtension re = (RouteExtension) event.getExtensions()
            .get(RouteExtension.Format.IN_MEMORY_KEY);
        if (!re.getRoute().isEmpty()) {
          return re.getRoute();
        }
      }
      return "default";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void append(CloudEvent event) throws RuntimeException {
      // A default topic should have matched, even if no explicit routing is configured
      String topicName = route(event);

      counts.compute(topicName, (k, v) -> v == null ? 1 : v + 1);

      ProducerRecord<String, byte[]> result = marshal(event, builder, topicName, null);

    }


    // The following code is copied from the Cloudevents SDK as the cloudevent producer wraps an older producer interface.
    @SuppressWarnings("unchecked")
    private <T> Wire<byte[], String, byte[]> marshal(Supplier<CloudEvent<AttributesImpl, T>> event,
        EventStep<AttributesImpl, T, byte[], byte[]> builder) {

      return Optional.ofNullable(builder)
          .map(step -> step.withEvent(event))
          .map(marshaller -> marshaller.marshal())
          .get();

    }

    private Set<Header> marshal(Map<String, byte[]> headers) {

      return headers.entrySet()
          .stream()
          .map(header -> new RecordHeader(header.getKey(), header.getValue()))
          .collect(Collectors.toSet());

    }

    private <T> ProducerRecord<String, byte[]> marshal(CloudEvent<AttributesImpl, T> event,
        EventStep<AttributesImpl, T, byte[], byte[]> builder,
        String topic,
        Integer partition) {
      Wire<byte[], String, byte[]> wire = marshal(() -> event, builder);
      Set<Header> headers = marshal(wire.getHeaders());

      Long timestamp = null;
      if (event.getAttributes().getTime().isPresent()) {
        timestamp = event.getAttributes().getTime().get().toInstant().toEpochMilli();
      }

      if (!wire.getPayload().isPresent()) {
        throw new RuntimeException("payload is empty");
      }

      byte[] payload = wire
          .getPayload()
          .get();

      return new ProducerRecord<>(
          topic,
          partition,
          timestamp,
          // Get partitionKey from cloudevent extensions once it is supported upstream.
          null,
          payload,
          headers);
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

  private static ConfluentAuditLogProvider providerWithCountExporter(String routerConfigJson) {
    try {
      Map<String, Object> configs = new HashMap<>();
      ConfluentAuditLogProvider provider = new ConfluentAuditLogProvider();
      configs.put(ROUTER_CONFIG, routerConfigJson);
      configs.put(AUTHORITY_NAME_PROP, "mds.example.com");
      configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      configs.put(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG, CountExporter.class.getName());
      provider.configure(configs);
      AuthorizerServerInfo serverInfo = new AuthorizerServerInfo() {
        @Override
        public ClusterResource clusterResource() {
          return new ClusterResource(CLUSTER_ID);
        }

        @Override
        public int brokerId() {
          return 0;
        }

        @Override
        public Collection<Endpoint> endpoints() {
          return null;
        }

        @Override
        public Endpoint interBrokerEndpoint() {
          return null;
        }
      };
      CompletableFuture<Void> startFuture = provider.start(serverInfo, configs)
          .toCompletableFuture();
      startFuture.get(10_000, TimeUnit.MILLISECONDS);
      return provider;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {

    for (int i = 0; i < DISTINCT_KEYS; i++) {
      Scope sourceScope = Scope.kafkaClusterScope(CLUSTER_ID);
      KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user" + i % USERS);
      ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"), "topic" + i % TOPICS,
          PatternType.LITERAL);
      RequestContext requestContext = new MockRequestContext(
          new RequestHeader(API_KEYS[i % API_KEYS.length], (short) 1, "", i), "",
          InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
          SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
      Action action = new Action(sourceScope, topic.resourceType(), topic.name(),
          new Operation(ACTIONS[i % ACTIONS.length]));
      AuthorizeResult authorizeResult =
          i % 2 == 0 ? AuthorizeResult.ALLOWED : AuthorizeResult.DENIED;
      AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
          principal);

      args[i] = new LogAuthorizationArguments(sourceScope, requestContext, action, authorizeResult,
          policy);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    System.out.println("Topic Deliveries:");
    CountExporter ce = (CountExporter) provider.getEventLogger().eventExporter();
    ce.counts.entrySet().stream()
        .sorted(Comparator.comparing(Entry::getKey))
        .forEach(e -> System.out.println(e.getKey() + "\t" + e.getValue()));
  }

  @Benchmark
  public void testLogAuthorizationNone() {
    provider = noneProvider;
    counter++;
    int index = (int) (counter % DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logAuthorization(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy);
  }

  @Benchmark
  public void testLogAuthorizationCreate() {
    provider = createProvider;
    counter++;
    int index = (int) (counter % DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logAuthorization(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy);
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceOne() {
    provider = createProduceOneLogProvider;
    counter++;
    int index = (int) (counter % DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logAuthorization(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy);
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceSome() {
    provider = createProduceSomeLogProvider;
    counter++;
    int index = (int) (counter % DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logAuthorization(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy);
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceAll() {
    provider = createProduceAllLogProvider;
    counter++;
    int index = (int) (counter % DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logAuthorization(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy);
  }

  @Benchmark
  public void testLogAuthorizationEverything() {
    provider = everythingLogProvider;
    counter++;
    int index = (int) (counter % DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logAuthorization(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ProviderLogAuthorizationBenchmark.class.getSimpleName())
        .forks(2)
        .build();

    new Runner(opt).run();
  }

}
