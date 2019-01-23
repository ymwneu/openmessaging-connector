package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.AllocateConnAndTaskStrategy;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.ConnectorAccessPoint;
import io.openmessaging.connect.runtime.Worker;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RebalanceImpl {

    private final Worker worker;
    private ConnAndTaskConfigs processConfigs = new ConnAndTaskConfigs();
    private final ConfigManagementService configManagementService;
    private final ClusterManagementService clusterManagementService;
    private ConnectorAccessPoint connectorAccessPoint;
    private AllocateConnAndTaskStrategy allocateConnAndTaskStrategy;
    public RebalanceImpl(Worker worker, ConfigManagementService configManagementService,
        ClusterManagementService clusterManagementService, AllocateConnAndTaskStrategy allocateConnAndTaskStrategy) {
        this.worker = worker;
        this.allocateConnAndTaskStrategy = allocateConnAndTaskStrategy;
        this.configManagementService = configManagementService;
        this.clusterManagementService = clusterManagementService;
    }

    public void doRebalance() {
        Set<String> curAliveWorkers = clusterManagementService.getAllAliveWorkers();
        Map<String, KeyValue> curConnectorConfigs = configManagementService.getConnectorConfigs();
        Map<String, List<KeyValue>> curTaskConfigs = configManagementService.getTaskConfigs();

        ConnAndTaskConfigs allocateResult = allocateConnAndTaskStrategy.allocate(curAliveWorkers, "", curConnectorConfigs, curTaskConfigs);

        updateProcessConfigsInRebalance(allocateResult);

    }

    private boolean updateProcessConfigsInRebalance(ConnAndTaskConfigs allocateResult) {

        // Reconfigure connectors
        for(String key : processConfigs.getConnectorConfigs().keySet()){

            // remove deleted connector
            // restart modified connector
        }
        for(String key : allocateResult.getConnectorConfigs().keySet()){

            // start new connector
        }

        // Reconfigure tasks
        for(String key : processConfigs.getTaskConfigs().keySet()){

        }
        for(String key : allocateResult.getTaskConfigs().keySet()){

        }
        return true;
    }


}
