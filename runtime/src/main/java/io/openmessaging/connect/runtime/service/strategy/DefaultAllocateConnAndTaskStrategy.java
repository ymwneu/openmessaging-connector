/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.openmessaging.connect.runtime.service.strategy;

import io.openmessaging.connect.runtime.common.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Default allocate strategy, distribute connectors and tasks averagely.
 */
public class DefaultAllocateConnAndTaskStrategy implements AllocateConnAndTaskStrategy {

    @Override
    public ConnAndTaskConfigs allocate(Set<String> allWorker, String curWorker, Map<String, ConnectKeyValue> connectorConfigs,
        Map<String, List<ConnectKeyValue>> taskConfigs) {
        ConnAndTaskConfigs allocateResult = new ConnAndTaskConfigs();
        if(null == allWorker || 0 == allWorker.size()){
            return allocateResult;
        }

        List<String> sortedWorkers = new ArrayList<>(allWorker);
        Collections.sort(sortedWorkers);
        Map<String, ConnectKeyValue> sortedConnectorConfigs = getSortedMap(connectorConfigs);
        Map<String, List<ConnectKeyValue>> sortedTaskConfigs = getSortedMap(taskConfigs);;
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
            for(ConnectKeyValue keyValue : sortedTaskConfigs.get(connectorName)){
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
