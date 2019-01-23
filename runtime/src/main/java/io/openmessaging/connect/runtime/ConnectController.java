package io.openmessaging.connect.runtime;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.service.ClusterManagementService;
import io.openmessaging.connect.runtime.service.ClusterManagementServiceImpl;
import io.openmessaging.connect.runtime.service.ConfigManagementService;
import io.openmessaging.connect.runtime.service.ConfigManagementServiceImpl;
import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connect.runtime.service.RebalanceImpl;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectController {

    private static final int TASK_THREAD_POOL_SIZE = 8;

    private final ConnectConfig connectConfig;
    private final ConfigManagementService configManagementService;
    private final PositionManagementService positionManagementService;
    private final ClusterManagementService clusterManagementService;
    private final Worker worker;
    private final RebalanceImpl rebalanceImpl;
    private final ExecutorService taskExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicBoolean stopping;
    private final MessagingAccessPoint messagingAccessPoint;

    public ConnectController(ConnectConfig connectConfig) {
        this.connectConfig = connectConfig;
        this.messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://alice@rocketmq.apache.org/us-east");
        positionManagementService = null;
        this.worker = new Worker(positionManagementService, messagingAccessPoint);
        this.clusterManagementService = new ClusterManagementServiceImpl(worker, new WorkerStatusListenerImpl());
        this.configManagementService = new ConfigManagementServiceImpl(new ConnectorConnectorConfigChangeListenerImpl());
        taskExecutor = new ThreadPoolExecutor(TASK_THREAD_POOL_SIZE, TASK_THREAD_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(),
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable herder) {
                    return new Thread(herder, "ConnectTaskExecutor");
                }
            });
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "ConnectScheduledThread");
                }
            });
        stopping = new AtomicBoolean(true);
        rebalanceImpl = new RebalanceImpl(worker, configManagementService, clusterManagementService, null);
    }

    public void initialize(){
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    ConnectController.this.configManagementService.persist();
                } catch (Exception e) {
                }
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    ConnectController.this.positionManagementService.persist();
                } catch (Exception e) {
                }
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);
    }

    public void start(){
        stopping.set(true);
    }


    public void stop(){
    }

    class WorkerStatusListenerImpl implements ClusterManagementService.WorkerStatusListener{

        @Override public void onWorkerChange() {
            rebalanceImpl.doRebalance();
        }
    }

    class ConnectorConnectorConfigChangeListenerImpl implements ConfigManagementService.ConnectorConfigUpdateListener {

        @Override public void onConfigUpdate() {
            rebalanceImpl.doRebalance();
        }
    }
}
