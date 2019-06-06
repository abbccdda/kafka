// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MetadataServiceWorker implements TaskWorker {
  private static final Logger log = LoggerFactory.getLogger(MetadataServiceWorker.class);

  private static final int THROTTLE_PERIOD_MS = 100;
  private static final String TEST_USER_NAME = "test_username";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Time TIME = Time.SYSTEM;

  private final String id;
  private final MetadataServiceWorkloadSpec spec;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Future<?> statusUpdaterFuture;
  private ExecutorService workerExecutor;
  private ScheduledExecutorService statusUpdaterExecutor;
  private WorkerStatusTracker status;
  private KafkaFutureImpl<String> doneFuture;
  private Platform platform;

  private String clusterId;
  private List<String> activeMetadataServerUrls;

  private long totalAuthorizeCalls;
  private long totalFailedCalls;
  private long startTimeMs;

  public MetadataServiceWorker(String id, MetadataServiceWorkloadSpec spec) {
    this.id = id;
    this.spec = spec;
  }

  @Override
  public void start(Platform platform, WorkerStatusTracker status,
                    KafkaFutureImpl<String> doneFuture) {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("MetadataServiceWorker is already running.");
    }

    synchronized (MetadataServiceWorker.this) {
      this.totalAuthorizeCalls = 0;
      this.totalFailedCalls = 0;
      this.startTimeMs = TIME.milliseconds();
    }

    log.info("{}: Activating MetadataServiceWorker.", id);

    try {
      this.platform = platform;
      this.status = status;
      this.doneFuture = doneFuture;
      validateConfigs();

      clusterId = clusterId();
      status.update(new TextNode("Got clusterId :" + clusterId));

      createRoleBindings();
      status.update(new TextNode("Created test RoleBindings for clusterId " + clusterId));

      activeMetadataServerUrls = activeMetadataServerUrls();
      status.update(new TextNode("Got active metadata server Urls : " + activeMetadataServerUrls));

      this.workerExecutor = Executors.newFixedThreadPool(activeMetadataServerUrls.size(),
          ThreadUtils.createThreadFactory("MetadataServiceWorker%d", false));
      for (int i = 0; i < activeMetadataServerUrls.size(); i++) {
        this.workerExecutor.submit(new Worker(i));
      }

      statusUpdaterExecutor = Executors.newScheduledThreadPool(1,
          ThreadUtils.createThreadFactory("StatusUpdaterWorkerThread%d", false));
      statusUpdaterFuture = statusUpdaterExecutor.scheduleAtFixedRate(
          new StatusUpdater(), 30, 30, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      WorkerUtils.abort(log, "MetadataServiceWorker", e, doneFuture);
    }
  }

  private void validateConfigs() {
    if (spec.targetCallsPerSec() <= 0) {
      throw new ConfigException("Can't have targetCallsPerSec <= 0.");
    }
    if (spec.numRoleBindings() <= 0) {
      throw new ConfigException("Can't have numRoleBindings <= 0.");
    }
    if (spec.adminUserCredentials() == null || spec.adminUserCredentials().length() == 0) {
      throw new ConfigException("adminUserCredentials can't be empty.");
    }
    if (spec.seedMetadataServerUrl() == null || spec.seedMetadataServerUrl().length() == 0) {
      throw new ConfigException("seedMetadataServerUrl can't be empty.");
    }
  }

  private String clusterId() throws Throwable {
    String url = String.format("%s/security/1.0/metadataClusterId", spec.seedMetadataServerUrl());
    String[] command = new String[] {"curl", "-sS", "-f", "-X", "GET", "--user", spec.adminUserCredentials(),
        "-H", "Content-Type:application/json", url};
    return platform.runCommand(command);
  }

  private void createRoleBindings() throws Throwable {
    Set<String> topicNames = IntStream.range(0, spec.numRoleBindings())
        .mapToObj(i -> "Topic" + i)
        .collect(Collectors.toSet());

    String url = String.format("%s/security/1.0/principals/User:%s/roles/ResourceOwner/bindings",
        spec.seedMetadataServerUrl(), TEST_USER_NAME);

    for (String topic : topicNames) {
      Map<String, Object> params = new LinkedHashMap<>();
      Map<String, Object> scopeParams = new HashMap<>();
      scopeParams.put("clusters", Collections.singletonMap("kafka-cluster", clusterId));
      params.put("scope", scopeParams);

      Map<String, Object> resourcePatternParams = new HashMap<>();
      resourcePatternParams.put("resourceType", "Topic");
      resourcePatternParams.put("name", topic);
      resourcePatternParams.put("patternType", "LITERAL");
      params.put("resourcePatterns", Collections.singletonList(resourcePatternParams));

      String[] command = new String[] {"curl", "-sS", "-f", "-X", "POST", "--user", spec.adminUserCredentials(),
          "-H", "Content-Type:application/json", "--data", OBJECT_MAPPER.writeValueAsString(params), url};
      platform.runCommand(command);
    }
  }

  private List<String> activeMetadataServerUrls() throws Throwable {
    String url = String.format("%s/security/1.0/activenodes/http", spec.seedMetadataServerUrl());
    String[] command = new String[] {"curl", "-sS", "-f", "-X", "GET", "--user", spec.adminUserCredentials(),
        "-H", "Content-Type:application/json", url};
    String jsonOutput = platform.runCommand(command);
    return OBJECT_MAPPER.readValue(jsonOutput, new TypeReference<List<String>>() {
    });
  }

  private String authorize(String url, String topic) throws Throwable {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("userPrincipal", "User:" + TEST_USER_NAME);
    Map<String, Object> actionParams = new HashMap<>();
    Map<String, Object> scopeParams = new HashMap<>();
    scopeParams.put("clusters", Collections.singletonMap("kafka-cluster", clusterId));
    actionParams.put("scope", scopeParams);
    actionParams.put("resourceName", topic);
    actionParams.put("resourceType", "Topic");
    actionParams.put("operation", "Read");
    params.put("actions", Collections.singletonList(actionParams));

    String authorizeUrl = String.format("%s/security/1.0/authorize", url);
    String[] command = new String[] {"curl", "-sS", "-f", "-X", "PUT", "--user", spec.adminUserCredentials(),
        "-H", "Content-Type:application/json", "--data", OBJECT_MAPPER.writeValueAsString(params), authorizeUrl};

    try {
      List<String> results = OBJECT_MAPPER.readValue(platform.runCommand(command), new TypeReference<List<String>>() {
      });
      return results.get(0);
    } catch (IOException e) {
      status.update(new TextNode("Error while authorize call : " + e.getMessage()));
      return "UNKNOWN_ERROR";
    }
  }

  class Worker implements Runnable {

    private final int index;

    Worker(int id) {
      this.index = id;
    }

    @Override
    public void run() {
      int perPeriod = WorkerUtils.perSecToPerPeriod(((float) spec.targetCallsPerSec()) /
          activeMetadataServerUrls.size(), THROTTLE_PERIOD_MS);
      Throttle throttle = new Throttle(perPeriod, THROTTLE_PERIOD_MS);

      try {
        int totalNumberOfTopics = spec.numRoleBindings();
        List<String> topicNames = IntStream.range(0, totalNumberOfTopics)
            .mapToObj(i -> "Topic" + i)
            .collect(Collectors.toList());

        int topicIndex = totalNumberOfTopics - 1;
        while (!doneFuture.isDone()) {
          throttle.increment();
          topicIndex = (topicIndex + 1) % totalNumberOfTopics;

          String result = authorize(activeMetadataServerUrls.get(index), topicNames.get(topicIndex));
          boolean success = true;
          if (!"ALLOWED".equalsIgnoreCase(result))
            success = false;

          synchronized (MetadataServiceWorker.this) {
            totalAuthorizeCalls++;
            if (!success) {
              totalFailedCalls++;
            }
          }
        }
      } catch (Throwable e) {
        WorkerUtils.abort(log, "MetadataServiceWorker#Worker", e, doneFuture);
      }
    }
  }

  private class StatusUpdater implements Runnable {
    @Override
    public void run() {
      try {
        long lastTimeMs = Time.SYSTEM.milliseconds();
        JsonNode node = JsonUtil.JSON_SERDE.valueToTree(
            new MetadataServiceWorker.StatusData(totalAuthorizeCalls, totalFailedCalls,
                (totalAuthorizeCalls * 1000.0) / (lastTimeMs - startTimeMs)));
        status.update(node);
      } catch (Exception e) {
        WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
      }
    }
  }

  public static class StatusData {
    private final long totalAuthorizeCalls;
    private final long totalFailedCalls;
    private final double callsPerSec;

    @JsonCreator
    StatusData(@JsonProperty("totalAuthorizeCalls") long totalAuthorizeCalls,
               @JsonProperty("totalFailedCalls") long totalFailedCalls,
               @JsonProperty("callsPerSec") double callsPerSec) {
      this.totalAuthorizeCalls = totalAuthorizeCalls;
      this.totalFailedCalls = totalFailedCalls;
      this.callsPerSec = callsPerSec;
    }

    @JsonProperty
    public long totalAuthorizeCalls() {
      return totalAuthorizeCalls;
    }

    @JsonProperty
    public long totalFailedCalls() {
      return totalFailedCalls;
    }

    @JsonProperty
    public double callsPerSec() {
      return callsPerSec;
    }
  }

  @Override
  public void stop(Platform platform) throws Exception {
    if (!running.compareAndSet(true, false)) {
      throw new IllegalStateException("MetadataServiceWorker is not running.");
    }
    log.info("{}: Deactivating MetadataServiceWorker.", id);
    doneFuture.complete("");

    this.statusUpdaterFuture.cancel(false);
    this.statusUpdaterExecutor.shutdown();
    this.statusUpdaterExecutor.awaitTermination(1, TimeUnit.DAYS);
    this.statusUpdaterExecutor = null;

    workerExecutor.shutdownNow();
    workerExecutor.awaitTermination(1, TimeUnit.DAYS);
    new StatusUpdater().run();
    this.workerExecutor = null;
    this.doneFuture = null;
    log.info("{}: Deactivated MetadataServiceWorker.", id);
  }
}
