/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.detector;

/**
 * The interface for an anomaly.
 */
public interface Anomaly {

  /**
   * @return A unique identifier for the anomaly.
   */
  String anomalyId();

  /**
   * Fix the anomaly with the system.
   *
   * @return True if fix was started successfully, false otherwise.
   */
  boolean fix() throws Exception;
}
