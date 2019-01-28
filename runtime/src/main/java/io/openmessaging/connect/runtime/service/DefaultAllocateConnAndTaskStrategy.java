package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultAllocateConnAndTaskStrategy implements AllocateConnAndTaskStrategy {
    @Override
    public ConnAndTaskConfigs allocate(Set<String> allWorker, String curWorker, Map<String, KeyValue> connectorConfigs,
        Map<String, List<KeyValue>> taskConfigs) {
        ConnAndTaskConfigs allocateResult = new ConnAndTaskConfigs();

        List<String> sortedWorkers = new ArrayList<>(allWorker);
        Collections.sort(sortedWorkers);

        if(sortedWorkers.get(0).equals(curWorker)){
            allocateResult.setConnectorConfigs(connectorConfigs);
            allocateResult.setTaskConfigs(taskConfigs);
        }
        return allocateResult;
    }
}
