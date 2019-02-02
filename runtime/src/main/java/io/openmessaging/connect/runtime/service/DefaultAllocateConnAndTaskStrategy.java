package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DefaultAllocateConnAndTaskStrategy implements AllocateConnAndTaskStrategy {
    @Override
    public ConnAndTaskConfigs allocate(Set<String> allWorker, String curWorker, Map<String, KeyValue> connectorConfigs,
        Map<String, List<KeyValue>> taskConfigs) {
        ConnAndTaskConfigs allocateResult = new ConnAndTaskConfigs();

        List<String> sortedWorkers = new ArrayList<>(allWorker);
        Collections.sort(sortedWorkers);
        Map<String, KeyValue> sortedConnectorConfigs = getSortedMap(connectorConfigs);
        Map<String, List<KeyValue>> sortedTaskConfigs = getSortedMap(taskConfigs);;
        int index = 0;
        for(String connectorName : sortedConnectorConfigs.keySet()){
            String allocatedWorker = sortedWorkers.get(index%sortedWorkers.size());
            index++;
            if(!curWorker.equals(allocatedWorker)){
                continue;
            }
            allocateResult.getConnectorConfigs().put(connectorName, sortedConnectorConfigs.get(connectorName));
        }
        for(String connectorName : sortedTaskConfigs.keySet()){
            for(KeyValue keyValue : sortedTaskConfigs.get(connectorName)){
                String allocatedWorker = sortedWorkers.get(index%sortedWorkers.size());
                index++;
                if(!curWorker.equals(allocatedWorker)){
                    continue;
                }
                if(null == allocateResult.getTaskConfigs().get(connectorName)){
                    allocateResult.getTaskConfigs().put(connectorName, new ArrayList<>());
                }
                allocateResult.getTaskConfigs().get(connectorName).add(keyValue);
            }
        }
        return allocateResult;
    }

    private <T> Map<String, T> getSortedMap(Map<String, T> map){

        Map<String, T> sortedMap = new TreeMap<>();
        for(String key : map.keySet()){
            sortedMap.put(key, map.get(key));
        }
        return sortedMap;
    }
}
