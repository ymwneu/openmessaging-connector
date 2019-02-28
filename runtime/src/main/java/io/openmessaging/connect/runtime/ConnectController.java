package io.openmessaging.connect.runtime;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.connectorwrapper.Worker;
import io.openmessaging.connect.runtime.rest.RestHandler;
import io.openmessaging.connect.runtime.service.ClusterManagementService;
import io.openmessaging.connect.runtime.service.ClusterManagementServiceImpl;
import io.openmessaging.connect.runtime.service.ConfigManagementService;
import io.openmessaging.connect.runtime.service.ConfigManagementServiceImpl;
import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connect.runtime.service.PositionManagementServiceImpl;
import io.openmessaging.connect.runtime.service.RebalanceImpl;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private final ConnectConfig connectConfig;
    private final ConfigManagementService configManagementService;
    private final PositionManagementService positionManagementService;
    private final ClusterManagementService clusterManagementService;
    private final Worker worker;
    private final MessagingAccessPoint messagingAccessPoint;
    private final RestHandler restHandler;
    private final RebalanceImpl rebalanceImpl;
    private ScheduledExecutorService scheduledExecutorService;

    public ConnectController(ConnectConfig connectConfig) {

        this.connectConfig = connectConfig;
        this.messagingAccessPoint = OMS.getMessagingAccessPoint(connectConfig.getOmsDriverUrl());
        this.clusterManagementService = new ClusterManagementServiceImpl(connectConfig, messagingAccessPoint);
        this.configManagementService = new ConfigManagementServiceImpl(connectConfig, messagingAccessPoint);
        this.positionManagementService = new PositionManagementServiceImpl(connectConfig, messagingAccessPoint);
        this.worker = new Worker(connectConfig, positionManagementService, messagingAccessPoint);
        this.rebalanceImpl = new RebalanceImpl(worker, configManagementService, clusterManagementService);
        restHandler = new RestHandler(this);
    }

    public void initialize(){

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((r) -> new Thread(r, "ConnectScheduledThread"));
    }

    public void start(){

        messagingAccessPoint.startup();
        clusterManagementService.start();
        configManagementService.start();
        positionManagementService.start();
        worker.start();
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                ConnectController.this.configManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist config error.", e);
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                ConnectController.this.positionManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist position error.", e);
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){

        if(messagingAccessPoint != null){
            messagingAccessPoint.shutdown();
        }

        if(clusterManagementService != null){
            clusterManagementService.stop();
        }

        if(configManagementService != null){
            configManagementService.stop();
        }

        if(positionManagementService != null){
            positionManagementService.stop();
        }

        if(worker != null){
            worker.stop();
        }

        if(configManagementService != null){
            configManagementService.persist();
        }

        if(positionManagementService != null){
            positionManagementService.persist();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("shutdown scheduledExecutorService error.", e);
        }
    }

    public ConnectConfig getConnectConfig() {
        return connectConfig;
    }

    public ConfigManagementService getConfigManagementService() {
        return configManagementService;
    }

    public PositionManagementService getPositionManagementService() {
        return positionManagementService;
    }

    public ClusterManagementService getClusterManagementService() {
        return clusterManagementService;
    }

    public Worker getWorker() {
        return worker;
    }

    public MessagingAccessPoint getMessagingAccessPoint() {
        return messagingAccessPoint;
    }

    public RestHandler getRestHandler() {
        return restHandler;
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }
}