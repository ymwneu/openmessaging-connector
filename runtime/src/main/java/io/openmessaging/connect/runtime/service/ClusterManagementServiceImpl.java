package io.openmessaging.connect.runtime.service;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import io.openmessaging.connector.api.sink.OMSQueue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ClusterManagementServiceImpl implements ClusterManagementService {

    private static final OMSQueue CLUSTER_MESSAGE_TOPIC = new OMSQueue("cluster-topic");
    private Map<String, Long> aliveWorker = new HashMap<>();

    private DataSynchronizer<String, Map<String, Long>> dataSynchronizer;
    private Set<ClusterManagementService.WorkerStatusListener> workerStatusListener;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ConnectConfig connectConfig;

    public ClusterManagementServiceImpl(ConnectConfig connectConfig, MessagingAccessPoint messagingAccessPoint) {
        this.connectConfig = connectConfig;
        this.dataSynchronizer = new BrokerBasedLog<>(messagingAccessPoint,
                                                     CLUSTER_MESSAGE_TOPIC,
                                                     connectConfig.getWorkerName(),
                                                     new ClusterChangeCallback());
        this.workerStatusListener = new HashSet<>();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "HeartBeatScheduledThread");
            }
        });
    }

    @Override
    public void start(){

        dataSynchronizer.start();

        // on worker online
        sendOnLineHeartBeat();

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {

                // check whether a machine is offline
                for(String workerId : aliveWorker.keySet()){
                    if((aliveWorker.get(workerId) + 30000) < System.currentTimeMillis()){
                        aliveWorker.remove(workerId);
                    }
                }
            } catch (Exception e) {
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                sendAliveHeartBeat();
            } catch (Exception e) {
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop(){
        sendOffLineHeartBeat();
    }

    public void sendAliveHeartBeat() {

        aliveWorker.put(connectConfig.getWorkerName(), System.currentTimeMillis());
//        dataSynchronizer.send("AliveHeartBeat", aliveWorker);
    }

    public void sendOnLineHeartBeat(){
        aliveWorker.put(connectConfig.getWorkerName(), System.currentTimeMillis());
//        dataSynchronizer.send("AliveHeartBeat", aliveWorker);
    }

    public void sendOffLineHeartBeat(){

        // dataSynchronizer.send();
    }

    @Override
    public Map<String, Long> getAllAliveWorkers() {
        return this.aliveWorker;
    }

    @Override
    public void registerListener(WorkerStatusListener listener) {
        this.workerStatusListener.add(listener);
    }

    private class ClusterChangeCallback implements Callback<String, Map<String, Long>> {

        @Override public void onCompletion(Throwable error, String key, Map<String, Long> result) {

            switch(key){

                default:
                    break;
            }
            for(WorkerStatusListener listener : ClusterManagementServiceImpl.this.workerStatusListener){
                listener.onWorkerChange();
            }
        }
    }
}
