package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.Worker;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.utils.ConnectorAccessPoint;
import java.util.List;
import java.util.Map;

public class RebalanceImpl {

    private final Worker worker;
    private ConnAndTaskConfigs processConfigs = new ConnAndTaskConfigs();
    private final ConfigManagementService configManagementService;
    private final ClusterManagementService clusterManagementService;
    private ConnectorAccessPoint connectorAccessPoint;
    private AllocateConnAndTaskStrategy allocateConnAndTaskStrategy;
    public RebalanceImpl(ConnectConfig connectorConfig, Worker worker, ConfigManagementService configManagementService,
        ClusterManagementService clusterManagementService) {

        this.worker = worker;
        this.configManagementService = configManagementService;
        this.clusterManagementService = clusterManagementService;
        this.configManagementService.registerListener(new ConnectorConnectorConfigChangeListenerImpl());
        this.clusterManagementService.registerListener(new WorkerStatusListenerImpl());
        this.allocateConnAndTaskStrategy = new DefaultAllocateConnAndTaskStrategy();
    }

    public void doRebalance() {

        Map<String, Long> curAliveWorkers = clusterManagementService.getAllAliveWorkers();
        Map<String, KeyValue> curConnectorConfigs = configManagementService.getConnectorConfigs();
        Map<String, List<KeyValue>> curTaskConfigs = configManagementService.getTaskConfigs();

        ConnAndTaskConfigs allocateResult = allocateConnAndTaskStrategy.allocate(curAliveWorkers.keySet(), worker.getWorkerId(), curConnectorConfigs, curTaskConfigs);
        System.out.println("allocateResult:"+ allocateResult.getConnectorConfigs());
        //        updateProcessConfigsInRebalance(allocateResult);
    }

    private void updateProcessConfigsInRebalance(ConnAndTaskConfigs allocateResult) {

        try{
            worker.startConnectors(allocateResult.getConnectorConfigs());
            worker.startTasks(allocateResult.getTaskConfigs());
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    class WorkerStatusListenerImpl implements ClusterManagementService.WorkerStatusListener{

        @Override public void onWorkerChange() {
            RebalanceImpl.this.doRebalance();
        }
    }

    class ConnectorConnectorConfigChangeListenerImpl implements ConfigManagementService.ConnectorConfigUpdateListener {

        @Override public void onConfigUpdate() {
            RebalanceImpl.this.doRebalance();
        }
    }
}
