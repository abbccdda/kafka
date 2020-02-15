/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class AclBenchWorker implements TaskWorker {
  private static final Logger log = LoggerFactory.getLogger(AclBenchWorker.class);

  private static final int THROTTLE_PERIOD_MS = 100;
  private static final Time TIME = Time.SYSTEM;
  private static final AtomicLong COUNTER = new AtomicLong(0);

  private final String id;
  private final AclWorkloadSpec spec;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Future<?> statusUpdaterFuture;
  private ExecutorService workerExecutor;
  private ScheduledExecutorService statusUpdaterExecutor;
  private WorkerStatusTracker status;
  private KafkaFutureImpl<String> doneFuture;

  private long totalCalls;
  private long startTimeMs;

  public AclBenchWorker(String id, AclWorkloadSpec spec) {
    this.id = id;
    this.spec = spec;
  }

  @Override
  public void start(Platform platform, WorkerStatusTracker status,
                    KafkaFutureImpl<String> doneFuture) {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("AclWorkloadSpec is already running.");
    }

    synchronized (AclBenchWorker.this) {
      this.totalCalls = 0;
      this.startTimeMs = TIME.milliseconds();
    }

    log.info("{}: Activating AclWorkloadSpec.", id);

    try {
      this.status = status;
      this.doneFuture = doneFuture;
      validateConfigs();

      this.workerExecutor = Executors.newFixedThreadPool(spec.noOfThreads(),
          ThreadUtils.createThreadFactory("AclBenchWorker%d", false));
      for (int i = 0; i < spec.noOfThreads(); i++) {
        this.workerExecutor.submit(new Worker());
      }

      statusUpdaterExecutor = Executors.newScheduledThreadPool(1,
          ThreadUtils.createThreadFactory("StatusUpdaterWorkerThread%d", false));
      statusUpdaterFuture = statusUpdaterExecutor.scheduleAtFixedRate(
          new StatusUpdater(), 30, 10, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      WorkerUtils.abort(log, "AclBenchWorker", e, doneFuture);
    }
  }

  private void validateConfigs() {
    if (spec.targetOperationsPerSec() <= 0) {
      throw new ConfigException("Can't have targetOperationsPerSec <= 0.");
    }
  }

  class Worker implements Runnable {
    @Override
    public void run() {
      int perPeriod = WorkerUtils.perSecToPerPeriod(((float) spec.targetOperationsPerSec()) /
          spec.noOfThreads(), THROTTLE_PERIOD_MS);
      Throttle throttle = new Throttle(perPeriod, THROTTLE_PERIOD_MS);
      AdminClient adminClient = null;
      try {
         adminClient =  createAdminClient();
        while (!doneFuture.isDone()) {
          throttle.increment();
          if (spec.aclDeletes()) {
            throttle.increment();
          }

          int noOfOps = runAclOperations(adminClient, COUNTER.incrementAndGet());

          synchronized (AclBenchWorker.this) {
            totalCalls = totalCalls + noOfOps;
          }
        }
      } catch (Throwable e) {
        Utils.closeQuietly(adminClient, "AdminClient");
        WorkerUtils.abort(log, "AclBenchWorker#Worker", e, doneFuture);
      }
    }
  }

  private int runAclOperations(final AdminClient adminClient, final long index) throws ExecutionException, InterruptedException {
    AclBinding aclBinding = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "Topic-" + index, PatternType.PREFIXED),
        new AccessControlEntry("User:User-" + index, "*", AclOperation.WRITE, AclPermissionType.ALLOW));
    log.debug("Adding aclBinding: {} ", aclBinding);
    adminClient.createAcls(Collections.singletonList(aclBinding)).all().get();

    if (spec.aclDeletes()) {
      log.debug("Deleting aclBinding: {} ", aclBinding);
      adminClient.deleteAcls(Collections.singletonList(aclBinding.toFilter())).all().get();
      return 2;
    }
    return 1;
  }


  private AdminClient createAdminClient() {
    Map<String, Object> props = new HashMap<>();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());

    if (spec.adminJaasConfig() != null && !spec.adminJaasConfig().isEmpty())
      props.put(SaslConfigs.SASL_JAAS_CONFIG, spec.adminJaasConfig());

    if (spec.saslMechanism() != null && !spec.saslMechanism().isEmpty())
      props.put(SaslConfigs.SASL_MECHANISM, spec.saslMechanism());

    if (spec.securityProtocol() != null && !spec.securityProtocol().isEmpty())
      props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, spec.securityProtocol());

    if (spec.saslMechanism() != null && spec.saslMechanism().equals("OAUTHBEARER"))
      props.put("sasl.login.callback.handler.class", "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler");

    return AdminClient.create(props);
  }

  private class StatusUpdater implements Runnable {
    @Override
    public void run() {
      try {
        long lastTimeMs = Time.SYSTEM.milliseconds();
        JsonNode node = JsonUtil.JSON_SERDE.valueToTree(
            new AclBenchWorker.StatusData(totalCalls,
                (totalCalls * 1000.0) / (lastTimeMs - startTimeMs)));
        status.update(node);
      } catch (Exception e) {
        WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
      }
    }
  }

  public static class StatusData {
    private final double callsPerSec;
    private final double totalCalls;

    @JsonCreator
    StatusData(@JsonProperty("totalCalls") double totalCalls,
               @JsonProperty("callsPerSec") double callsPerSec) {
      this.callsPerSec = callsPerSec;
      this.totalCalls = totalCalls;
    }

    @JsonProperty
    public double callsPerSec() {
      return callsPerSec;
    }

    @JsonProperty
    public double totalCalls() {
      return totalCalls;
    }
  }

  @Override
  public void stop(Platform platform) throws Exception {
    if (!running.compareAndSet(true, false)) {
      throw new IllegalStateException("AclBenchWorker is not running.");
    }
    log.info("{}: Deactivating AclBenchWorker.", id);
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

    long lastTimeMs = Time.SYSTEM.milliseconds();
    double callsPerSec = (totalCalls * 1000.0) / (lastTimeMs - startTimeMs);
    log.info("Achieved CallsPerSec : {}, minSupportedOpsPerSec : {}", callsPerSec, spec.minSupportedOpsPerSec());
    if (callsPerSec < spec.minSupportedOpsPerSec())
      throw new RuntimeException("Minimum supported operations/sec is " + spec.minSupportedOpsPerSec() + " , but got " + callsPerSec);

    log.info("{}: Deactivated AclBenchWorker.", id);
  }
}
