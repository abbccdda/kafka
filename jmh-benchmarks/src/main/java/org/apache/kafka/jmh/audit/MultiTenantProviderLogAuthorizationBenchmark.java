package org.apache.kafka.jmh.audit;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.multitenant.utils.TenantSanitizer;
import io.confluent.security.audit.provider.ConfluentAuditLogProvider;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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
 * Benchmarks for tuning Audit Logging performance. This calls the logEvent
 * method with a number of different configurations:
 * - not logging any messages (with the defaults set to "")
 *   This measures the cost of creating an AuditLogEntry and using the router
 *   to determine its disposition
 * - Logging only Create authorizations
 *   This simulates the default "Management only" auditing, with 1/33 of the
 *   authorizations being "Management" operations
 * - Logging Create authorizations and 1/97 of Produce authorizations
 *   Very selective Produce logging
 * - Logging Create and 11/97 of Produce authorizations
 *   Not selective Produce logging
 * - Logging Create and 97/97 of Produce authorizations
 *   Comprehensive Produce logging
 * - Logging Everything
 *   Comprehensive Produce and consume logging
 */

@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MultiTenantProviderLogAuthorizationBenchmark {

  private static final String TENANT_ID = "lkc-1234";

  private long counter = 0;

  private final LogAuthorizationArguments[] args = new LogAuthorizationArguments[LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS];
  private ConfluentAuditLogProvider provider;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    TenantMetadata metadata = new TenantMetadata(TENANT_ID, TENANT_ID);
    for (int i = 0; i < LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS; i++) {
      Scope scope = Scope.kafkaClusterScope(LogAuthorizationBenchmarkDefaults.CLUSTER_ID);
      KafkaPrincipal principal =
          new MultiTenantPrincipal("" + i % LogAuthorizationBenchmarkDefaults.USERS, metadata);
      ResourcePattern topic = new ResourcePattern(new ResourceType("Topic"),
          TENANT_ID + "_topic" + i % LogAuthorizationBenchmarkDefaults.TOPICS,
          PatternType.LITERAL);
      RequestContext context = new MockRequestContext(
          new RequestHeader(
              LogAuthorizationBenchmarkDefaults.API_KEYS[i
                  % LogAuthorizationBenchmarkDefaults.API_KEYS.length], (short) 1, "", i), "",
          InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("EXTERNAL"),
          SecurityProtocol.SASL_SSL, RequestContext.KAFKA);
      Action action = new Action(scope, topic.resourceType(), topic.name(),
          new Operation(
              LogAuthorizationBenchmarkDefaults.ACTIONS[i
                  % LogAuthorizationBenchmarkDefaults.ACTIONS.length]));
      AuthorizeResult result =
          i % 2 == 0 ? AuthorizeResult.ALLOWED : AuthorizeResult.DENIED;
      AuthorizePolicy policy = new AuthorizePolicy.SuperUser(AuthorizePolicy.PolicyType.SUPER_USER,
          principal);

      args[i] = new LogAuthorizationArguments(scope, context, action, result, policy);
    }

    LogAuthorizationBenchmarkDefaults.noneProvider.setSanitizer(
        TenantSanitizer::tenantAuditEvent);
    LogAuthorizationBenchmarkDefaults.createProvider.setSanitizer(
        TenantSanitizer::tenantAuditEvent);
    LogAuthorizationBenchmarkDefaults.createProduceOneLogProvider.setSanitizer(
        TenantSanitizer::tenantAuditEvent);
    LogAuthorizationBenchmarkDefaults.createProduceSomeLogProvider.setSanitizer(
        TenantSanitizer::tenantAuditEvent);
    LogAuthorizationBenchmarkDefaults.createProduceAllLogProvider.setSanitizer(
        TenantSanitizer::tenantAuditEvent);
    LogAuthorizationBenchmarkDefaults.everythingLogProvider.setSanitizer(
        TenantSanitizer::tenantAuditEvent);
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
    provider = LogAuthorizationBenchmarkDefaults.noneProvider;
    counter++;
    int index = (int) (counter % LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreate() {
    provider = LogAuthorizationBenchmarkDefaults.createProvider;
    counter++;
    int index = (int) (counter % LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceOne() {
    provider = LogAuthorizationBenchmarkDefaults.createProduceOneLogProvider;
    counter++;
    int index = (int) (counter % LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceSome() {
    provider = LogAuthorizationBenchmarkDefaults.createProduceSomeLogProvider;
    counter++;
    int index = (int) (counter % LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationCreateProduceAll() {
    provider = LogAuthorizationBenchmarkDefaults.createProduceAllLogProvider;
    counter++;
    int index = (int) (counter % LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  @Benchmark
  public void testLogAuthorizationEverything() {
    provider = LogAuthorizationBenchmarkDefaults.everythingLogProvider;
    counter++;
    int index = (int) (counter % LogAuthorizationBenchmarkDefaults.DISTINCT_KEYS);
    LogAuthorizationArguments arg = args[index];
    provider
        .logEvent(new ConfluentAuthorizationEvent(arg.sourceScope, arg.requestContext, arg.action, arg.authorizeResult,
            arg.authorizePolicy));
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(MultiTenantProviderLogAuthorizationBenchmark.class.getSimpleName())
        .forks(2)
        .build();

    new Runner(opt).run();
  }

}
