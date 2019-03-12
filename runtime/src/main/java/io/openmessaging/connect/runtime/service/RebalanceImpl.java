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

/**
 * Distribute connectors and tasks in current cluster.
 */
public class RebalanceImpl {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    /**
     * Worker to schedule connectors and tasks in current process.
     */
    private final Worker worker;

    /**
     * ConfigManagementService to access current config info.
     */
    private final ConfigManagementService configManagementService;

    /**
     * ClusterManagementService to access current cluster info.
     */
    private final ClusterManagementService clusterManagementService;

    /**
     * Strategy to allocate connectors and tasks.
     */
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

    /**
     * Distribute connectors and tasks according to the {@link RebalanceImpl#allocateConnAndTaskStrategy}.
     */
    public void doRebalance() {

        Map<String, Long> curAliveWorkers = clusterManagementService.getAllAliveWorkers();
        Map<String, ConnectKeyValue> curConnectorConfigs = configManagementService.getConnectorConfigs();
        Map<String, List<ConnectKeyValue>> curTaskConfigs = configManagementService.getTaskConfigs();

        ConnAndTaskConfigs allocateResult = allocateConnAndTaskStrategy.allocate(curAliveWorkers.keySet(), worker.getWorkerId(), curConnectorConfigs, curTaskConfigs);
        log.info("allocated connector:"+ allocateResult.getConnectorConfigs());
        log.info("allocated task:"+ allocateResult.getTaskConfigs());
        updateProcessConfigsInRebalance(allocateResult);
    }

    /**
     * Start all the connectors and tasks allocated to current process.
     * @param allocateResult
     */
    private void updateProcessConfigsInRebalance(ConnAndTaskConfigs allocateResult) {

        try{
            worker.startConnectors(allocateResult.getConnectorConfigs());
            worker.startTasks(allocateResult.getTaskConfigs());
        }catch(Exception e){
            log.error("RebalanceImpl#updateProcessConfigsInRebalance start connector or task failed", e);
        }
    }

    class WorkerStatusListenerImpl implements ClusterManagementService.WorkerStatusListener{

        /**
         * When alive workers change.
         */
        @Override
        public void onWorkerChange() {
            RebalanceImpl.this.doRebalance();
        }
    }

    class ConnectorConnectorConfigChangeListenerImpl implements ConfigManagementService.ConnectorConfigUpdateListener {

        /**
         * When config change.
         */
        @Override
        public void onConfigUpdate() {
            RebalanceImpl.this.doRebalance();
        }
    }
}
