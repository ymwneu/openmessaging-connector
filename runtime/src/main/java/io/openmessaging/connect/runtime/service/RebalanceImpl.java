package io.openmessaging.connect.runtime.service;

import io.openmessaging.connect.runtime.common.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.connectorwrapper.Worker;
import io.openmessaging.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import io.openmessaging.connect.runtime.service.strategy.DefaultAllocateConnAndTaskStrategy;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceImpl {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private final Worker worker;
    private final ConfigManagementService configManagementService;
    private final ClusterManagementService clusterManagementService;
    private AllocateConnAndTaskStrategy allocateConnAndTaskStrategy;
    public RebalanceImpl(Worker worker, ConfigManagementService configManagementService,
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
        Map<String, ConnectKeyValue> curConnectorConfigs = configManagementService.getConnectorConfigs();
        Map<String, List<ConnectKeyValue>> curTaskConfigs = configManagementService.getTaskConfigs();

        ConnAndTaskConfigs allocateResult = allocateConnAndTaskStrategy.allocate(curAliveWorkers.keySet(), worker.getWorkerId(), curConnectorConfigs, curTaskConfigs);
        log.info("allocated connector:"+ allocateResult.getConnectorConfigs());
        log.info("allocated task:"+ allocateResult.getTaskConfigs());
        updateProcessConfigsInRebalance(allocateResult);
    }

    private void updateProcessConfigsInRebalance(ConnAndTaskConfigs allocateResult) {

        try{
            worker.startConnectors(allocateResult.getConnectorConfigs());
            worker.startTasks(allocateResult.getTaskConfigs());
        }catch(Exception e){
            log.error("RebalanceImpl#updateProcessConfigsInRebalance start connector or task failed", e);
        }
    }

    class WorkerStatusListenerImpl implements ClusterManagementService.WorkerStatusListener{

        @Override
        public void onWorkerChange() {
            RebalanceImpl.this.doRebalance();
        }
    }

    class ConnectorConnectorConfigChangeListenerImpl implements ConfigManagementService.ConnectorConfigUpdateListener {

        @Override
        public void onConfigUpdate() {
            RebalanceImpl.this.doRebalance();
        }
    }
}
