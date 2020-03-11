/*
 Copyright 2019 Confluent Inc.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;

import java.util.HashMap;
import java.util.Map;

public class TestBrokerCapacityConfigResolver implements BrokerCapacityConfigResolver {
  private Map<Resource, Double> _capacityMap = new HashMap<>();
  private String _estimationInfo;

  @Override
  public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId) {
    return new BrokerCapacityInfo(_capacityMap, _estimationInfo);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _capacityMap.put(Resource.DISK, Double.valueOf((String) configs.get("test.disk.capacity")));
    _capacityMap.put(Resource.CPU, Double.valueOf((String) configs.get("test.cpu.capacity")));
    _capacityMap.put(Resource.NW_IN, Double.valueOf((String) configs.get("test.nwin.capacity")));
    _capacityMap.put(Resource.NW_OUT, Double.valueOf((String) configs.get("test.nwout.capacity")));

    _estimationInfo = (String) configs.get("estimation.info");
  }

  @Override
  public void close() throws Exception {

  }
}
