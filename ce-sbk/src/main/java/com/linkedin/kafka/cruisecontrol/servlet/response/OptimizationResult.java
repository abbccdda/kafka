/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OptimizationResult  {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizationResult.class);
  protected static final String SUMMARY = "summary";
  protected static final String PROPOSALS = "proposals";
  protected static final String GOAL = "goal";
  protected static final String GOAL_SUMMARY = "goalSummary";
  protected static final String STATUS = "status";
  protected static final String CLUSTER_MODEL_STATS = "clusterModelStats";
  protected static final String LOAD_AFTER_OPTIMIZATION = "loadAfterOptimization";
  protected static final String LOAD_BEFORE_OPTIMIZATION = "loadBeforeOptimization";
  protected static final String VIOLATED = "VIOLATED";
  protected static final String FIXED = "FIXED";
  protected static final String NO_ACTION = "NO-ACTION";

  // Needed for JSON generation.
  public static final int JSON_VERSION = 1;
  public static final String VERSION = "version";

  protected OptimizerResult _optimizerResult;
  protected String _cachedJSONResponse;
  protected String _cachedPlaintextResponse;
  protected String _cachedResponse;

  public OptimizationResult(OptimizerResult optimizerResult, KafkaCruiseControlConfig config) {
    _optimizerResult = optimizerResult;
    _cachedJSONResponse = null;
    _cachedPlaintextResponse = null;
  }

  public OptimizerResult optimizerResult() {
    return _optimizerResult;
  }

  /**
   * @return JSON response if cached, null otherwise.
   */

  public String cachedJSONResponse() {
    return _cachedJSONResponse;
  }

  /**
   * @return Plaintext response if cached, null otherwise.
   */

  public String cachedPlaintextResponse() {
    return _cachedPlaintextResponse;
  }

  protected String getPlaintextPretext() {
    return "Cluster load:";
  }

  protected String getPlaintext(boolean isVerbose, String pretext) {
    StringBuilder sb = new StringBuilder();
    if (isVerbose) {
      sb.append(_optimizerResult.goalProposals().toString());
    }

    writeProposalSummary(sb);
    if (isVerbose) {
      sb.append(String.format("%n%nCurrent load:%n%s", _optimizerResult.brokerStatsBeforeOptimization().toString()));
    }

    sb.append(String.format("%s%s", pretext, _optimizerResult.brokerStatsAfterOptimization().toString()));
    return sb.toString();
  }

  protected void discardIrrelevantAndCacheRelevant() {
    _cachedResponse = getPlaintext(false, "");
    // Discard irrelevant response.
    _optimizerResult = null;
  }

  /**
   * Keeps the JSON and plaintext response and discards the optimizer result.
   */
  public void discardIrrelevantAndCacheJsonAndPlaintext() {
    if (_optimizerResult != null) {
      _cachedJSONResponse = getJSONString(false);
      _cachedPlaintextResponse = getPlaintext(false, String.format("%n%nCluster load after self-healing:%n"));
      // Discard irrelevant response.
      _optimizerResult = null;
    }
  }

  protected String getJSONString(boolean isVerbose) {
    Map<String, Object> optimizationResult = new HashMap<>();
    if (isVerbose) {
      optimizationResult.put(PROPOSALS, _optimizerResult.goalProposals().stream()
                                                        .map(ExecutionProposal::getJsonStructure).collect(Collectors.toSet()));
      optimizationResult.put(LOAD_BEFORE_OPTIMIZATION, _optimizerResult.brokerStatsBeforeOptimization().getJsonStructure());
    }

    optimizationResult.put(SUMMARY, _optimizerResult.getProposalSummaryForJson());
    List<Map<String, Object>> goalSummary = new ArrayList<>();
    for (Map.Entry<String, ClusterModelStats> entry : _optimizerResult.statsByGoalName().entrySet()) {
      String goalName = entry.getKey();
      Map<String, Object> goalMap = new HashMap<>();
      goalMap.put(GOAL, goalName);
      goalMap.put(STATUS, goalResultDescription(goalName));
      goalMap.put(CLUSTER_MODEL_STATS, entry.getValue().getJsonStructure());
      goalSummary.add(goalMap);
    }
    optimizationResult.put(GOAL_SUMMARY, goalSummary);
    optimizationResult.put(LOAD_AFTER_OPTIMIZATION, _optimizerResult.brokerStatsAfterOptimization().getJsonStructure());
    optimizationResult.put(VERSION, JSON_VERSION);
    Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();

    return gson.toJson(optimizationResult);
  }

  protected void writeProposalSummary(StringBuilder sb) {
    sb.append(_optimizerResult.getProposalSummary());
    for (Map.Entry<String, ClusterModelStats> entry : _optimizerResult.statsByGoalName().entrySet()) {
      String goalName = entry.getKey();
      sb.append(String.format("%n%nStats for %s(%s):%n", goalName, goalResultDescription(goalName)));
      sb.append(entry.getValue().toString());
    }
  }

  protected String goalResultDescription(String goalName) {
    return _optimizerResult.violatedGoalsBeforeOptimization().contains(goalName) ?
           _optimizerResult.violatedGoalsAfterOptimization().contains(goalName) ? VIOLATED : FIXED : NO_ACTION;
  }

}
