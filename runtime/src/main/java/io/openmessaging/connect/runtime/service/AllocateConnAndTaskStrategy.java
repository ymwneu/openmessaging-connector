package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AllocateConnAndTaskStrategy {

    ConnAndTaskConfigs allocate(Set<String> allWorker, String curWorker, Map<String, KeyValue> connectorConfigs,
        Map<String, List<KeyValue>> taskConfigs);
}