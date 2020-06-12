/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class KafkaCruiseControlThreadFactory implements ThreadFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlThreadFactory.class);
  private final String _name;
  private final boolean _daemon;
  private final AtomicInteger _id = new AtomicInteger(0);
  private final Logger _logger;
  private final Optional<Integer> _brokerId;

  public KafkaCruiseControlThreadFactory(String name,
                                         boolean daemon,
                                         Logger logger) {
    this(name, daemon, logger, Optional.empty());
  }

  public KafkaCruiseControlThreadFactory(String name,
                                         boolean daemon,
                                         Logger logger,
                                         Optional<Integer> brokerId) {
    _name = "SBK_" + name;
    _daemon = daemon;
    _logger = logger == null ? LOG : logger;
    _brokerId = brokerId;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(() -> {
      _brokerId.ifPresent(brokerId -> MDC.put("brokerId", brokerId.toString()));
      r.run();
    }, _name + "-" + _id.getAndIncrement());
    t.setDaemon(_daemon);
    t.setUncaughtExceptionHandler((t1, e) -> _logger.error("Uncaught exception in " + t1.getName() + ": ", e));
    return t;
  }
}
