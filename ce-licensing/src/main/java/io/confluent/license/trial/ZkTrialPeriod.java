/*
 * Copyright [2016  - 2018] Confluent Inc.
 */

package io.confluent.license.trial;

import java.util.Date;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of a users trial period, with the state information stored on zookeeper.
 */
// Suppress warnings about using ZkUtils until we migrate to KafkaZkClient
@SuppressWarnings("deprecation")
public class ZkTrialPeriod {

  private static final Logger log = LoggerFactory.getLogger(ZkTrialPeriod.class);

  private static final String DEFAULT_TRIAL_PERIOD_ZK_PATH = "/confluent-license/trial";
  private static final int ZK_SESSION_TIMEOUT_MS = 30000;
  private static final int ZK_CONNECT_TIMEOUT_MS = 7000;
  private static final long TRIAL_LIMIT_MS = TimeUnit.DAYS.toMillis(30);

  private final String licensePath;
  private final String zkConnect;

  public ZkTrialPeriod(String zkConnect) {
    this(zkConnect,  DEFAULT_TRIAL_PERIOD_ZK_PATH);
  }

  public ZkTrialPeriod(String zkConnect, String licensePath) {
    this.zkConnect = zkConnect;
    this.licensePath = licensePath;
  }

  /**
   * Starts or verifies a trial period.
   * @param now current time as milliseconds since epoch
   *
   * @return the time remaining on the trial in milliseconds, or 0 if the trial has expired.
   */
  public long startOrVerify(long now) {
   try (ZkUtils zkUtils = new ZkUtils(
        zkConnect,
        ZK_SESSION_TIMEOUT_MS,
        ZK_CONNECT_TIMEOUT_MS,
        JaasUtils.isZkSecurityEnabled())) {
      return startOrVerifyTrial(zkUtils, now);
    }
  }

  private long startOrVerifyTrial(ZkUtils zkUtils, long now) {
    try {
      zkUtils.createPersistentPath(licensePath, "");
      log.info("Registered trial license with a validity period of {} days",
          TimeUnit.MILLISECONDS.toDays(TRIAL_LIMIT_MS));
      return TRIAL_LIMIT_MS;
    } catch (ZkNodeExistsException e) {
      // The node already exists, which means that the trial has already started.
      log.debug("Trial period has already started, checking elapsed time");
    }

    Stat stat = zkUtils.readStat(licensePath);

    // the Math.max is to compensate for clock skews.
    long trialStartTimeMs = stat.getCtime();
    long elapsedTime = Math.max(now - trialStartTimeMs, 0);
    long remainingMs = Math.max(TRIAL_LIMIT_MS - elapsedTime, 0);
    Date trialStartDate = new Date(trialStartTimeMs);
    if (remainingMs == 0) {
      log.error("Your trial license registered at {} has expired. "
          + "Please add a valid license to continue using the product", trialStartDate);
    } else {
      log.info("Using trial license registered at {} which expires in {} seconds",
          trialStartDate, TimeUnit.MILLISECONDS.toSeconds(remainingMs));
    }
    return remainingMs;
  }
}
