// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SchemaRegistryWorker implements TaskWorker {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryWorker.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Time TIME = Time.SYSTEM;

  private final String id;
  private final SchemaRegistryWorkloadSpec spec;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Future<?> statusUpdaterFuture;
  private ExecutorService workerExecutor;
  private ScheduledExecutorService statusUpdaterExecutor;
  private WorkerStatusTracker status;
  private KafkaFutureImpl<String> doneFuture;
  private Platform platform;

  // this set holds the set of subject identifiers to skip when attempting DELETE because
  // these identifiers failed when registering schemas
  private Set<Integer> skippedIdentifiers;

  private AtomicLong totalCalls;
  private AtomicLong totalFailedCalls;
  private AtomicLong startTimeMs;

  public SchemaRegistryWorker(String id, SchemaRegistryWorkloadSpec spec) {
    this.id = id;
    this.spec = spec;
  }

  @Override
  public void start(Platform platform, WorkerStatusTracker status,
                    KafkaFutureImpl<String> doneFuture) {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("SchemaRegistryWorker is already running");
    }

    this.totalCalls = new AtomicLong(0);
    this.totalFailedCalls = new AtomicLong(0);
    this.startTimeMs = new AtomicLong(TIME.milliseconds());
    this.skippedIdentifiers = ConcurrentHashMap.newKeySet();

    log.info("{}: Activating SchemaRegistryWorker.", id);

    try {
      this.platform = platform;
      this.status = status;
      this.doneFuture = doneFuture;

      validateConfigs();

      workerExecutor = Executors.newFixedThreadPool(1,
          ThreadUtils.createThreadFactory("SchemaRegistryWorker%d", false));
      workerExecutor.submit(new Worker());

      statusUpdaterExecutor = Executors.newScheduledThreadPool(1,
          ThreadUtils.createThreadFactory("StatusUpdaterWorkerThread%d", false));
      statusUpdaterFuture = statusUpdaterExecutor.scheduleAtFixedRate(
          new StatusUpdater(), 30, 30, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      WorkerUtils.abort(log, "SchemaRegistryWorker", e, doneFuture);
    }
  }

  private void validateConfigs() {
    if (spec.targetCallsPerSec() <= 0) {
      throw new ConfigException("Can't have targetCallsPerSec <= 0.");
    }
    if (spec.schemaRegistryUrl() == null || spec.schemaRegistryUrl().length() == 0) {
      throw new ConfigException("schemaRegistryUrl can't be empty.");
    }
    if (spec.numSchemas() <= 0) {
      throw new ConfigException("Can't have numSchemas <= 0.");
    }
  }

  private String registerSchema(int subjectIdentifier) throws Throwable {
    String url = String.format("%s/subjects/test-subject-%d/versions",
        spec.schemaRegistryUrl(), subjectIdentifier);
    log.info("Registering new schema to test-subject-{} at {}", subjectIdentifier, url);

    Map<String, String> schemaParams = Collections.singletonMap(
       "schema",
        String.format("{\"type\": \"record\", \"name\": \"test%d\", "
                + "\"fields\": [{\"type\": \"string\", \"name\": \"f%d\"}]}",
            subjectIdentifier, subjectIdentifier));

    String[] command = new String[]{"curl", "-X", "POST", "-H",
        "Content-Type:application/vnd.schemaregistry.v1+json", "--data",
        OBJECT_MAPPER.writeValueAsString(schemaParams), url};
    return platform.runCommand(command);
  }

  private String deleteSchema(int subjectIdentifier) throws Throwable {
    String url = String.format("%s/subjects/test-subject-%d/versions/1",
        spec.schemaRegistryUrl(), subjectIdentifier);
    log.info("Deleting new schema to test-subject-{} at {}", subjectIdentifier, url);
    String[] command = new String[]{"curl", "-X", "DELETE", url};
    return platform.runCommand(command);
  }

  @Override
  public void stop(Platform platform) throws Exception {
    if (!running.compareAndSet(true, false)) {
      throw new IllegalStateException("SchemaRegistryWorker is not running.");
    }

    log.info("{}: Deactivating SchemaRegistryWorker", id);
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
    log.info("{}: Deactivated SchemaRegistryWorker.", id);
  }

  class Worker implements Runnable {

    @Override
    public void run() {
      try {
        // registering schemas to different subjects
        for (int identifier = 0; identifier < spec.numSchemas(); identifier++) {
          String output = registerSchema(identifier);
          boolean success = true;
          log.info(output);

          // sometimes we don't want to parse result to see pure throughput
          if (spec.parseResult()) {
            try {
              log.info("ID is {}", OBJECT_MAPPER.readValue(output, RegisterSchemaResponse.class).getId());
            } catch (Exception e) {
              skippedIdentifiers.add(identifier);
              success = false;
            }
          }

          totalCalls.getAndIncrement();
          if (!success) {
            totalFailedCalls.getAndIncrement();
          }
        }

        // deleting schemas from different subjects
        for (int identifier = 0; identifier < spec.numSchemas(); identifier++) {
          if (!skippedIdentifiers.contains(identifier)) {
            boolean success = true;
            String output = deleteSchema(identifier);
            log.info(output);

            // sometimes we don't want to parse result to see pure throughput
            if (spec.parseResult()) {
              try {
                Integer.parseInt(output);
              } catch (Exception e) {
                success = false;
              }

            }

            totalCalls.getAndIncrement();
            if (!success) {
              totalFailedCalls.getAndIncrement();
            }
          }
        }
      } catch (Throwable e) {
        WorkerUtils.abort(log, "RegisterSchemas", e, doneFuture);
      }
      doneFuture.complete("");
    }

  }

  private class StatusUpdater implements Runnable {
    @Override
    public void run() {
      try {
        long lastTimeMs = Time.SYSTEM.milliseconds();
        JsonNode node = JsonUtil.JSON_SERDE.valueToTree(
            new SchemaRegistryWorker.StatusData(
                spec.schemaRegistryUrl(), totalCalls.get(), totalFailedCalls.get(),
                (totalCalls.get() * 1000.0) / (lastTimeMs - startTimeMs.get())));
        status.update(node);
      } catch (Exception e) {
        WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
      }
    }
  }

  public static class StatusData {
    private final long totalCalls;
    private final long totalFailedCalls;
    private final double callsPerSec;
    private final String schemaRegistryUrl;

    @JsonCreator
    StatusData(@JsonProperty("schemaRegistryUrl") String schemaRegistryUrl,
               @JsonProperty("totalCalls") long totalCalls,
               @JsonProperty("totalFailedCalls") long totalFailedCalls,
               @JsonProperty("callsPerSec") double callsPerSec) {
      this.schemaRegistryUrl = schemaRegistryUrl;
      this.totalCalls = totalCalls;
      this.totalFailedCalls = totalFailedCalls;
      this.callsPerSec = callsPerSec;
    }

    @JsonProperty
    public String schemaRegistryUrl() {
      return schemaRegistryUrl;
    }

    @JsonProperty
    public long totalCalls() {
      return totalCalls;
    }

    @JsonProperty
    public long totalFailedCalls() {
      return totalFailedCalls;
    }

    @JsonProperty
    public double callsPecSec() {
      return callsPerSec;
    }
  }

  private static class RegisterSchemaResponse {
    private int id;

    @JsonCreator
    public RegisterSchemaResponse(@JsonProperty("id") int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }
  }
}
