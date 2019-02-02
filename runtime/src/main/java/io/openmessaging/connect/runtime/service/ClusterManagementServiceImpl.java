package io.openmessaging.connect.runtime.service;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.utils.JsonConverter;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import io.openmessaging.connector.api.sink.OMSQueue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ClusterManagementServiceImpl implements ClusterManagementService {

    private static final OMSQueue CLUSTER_MESSAGE_TOPIC = new OMSQueue("cluster-topic", 0);
    private Map<String, Long> aliveWorker = new HashMap<>();

    private DataSynchronizer<String, Map> dataSynchronizer;
    private Set<ClusterManagementService.WorkerStatusListener> workerStatusListener;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ConnectConfig connectConfig;

    public ClusterManagementServiceImpl(ConnectConfig connectConfig, MessagingAccessPoint messagingAccessPoint) {
        this.connectConfig = connectConfig;
        this.dataSynchronizer = new BrokerBasedLog<>(messagingAccessPoint,
                                                     CLUSTER_MESSAGE_TOPIC,
                                                     connectConfig.getWorkerId()+System.currentTimeMillis(),
                                                     new ClusterChangeCallback(),
                                                     new JsonConverter(),
                                                     new JsonConverter(),
                                                     String.class,
                                                     Map.class);
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
        sendOnlineHeartBeat();

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {

                // check whether a machine is offline
                boolean changed = false;
                for(String workerId : aliveWorker.keySet()){
                    if((aliveWorker.get(workerId) + ClusterManagementService.WORKER_TIME_OUT) < System.currentTimeMillis()){
                        changed = true;
                        aliveWorker.remove(workerId);
                    }
                }
                if(!changed){
                    return;
                }
                for(WorkerStatusListener listener : ClusterManagementServiceImpl.this.workerStatusListener){
                    listener.onWorkerChange();
                }
            } catch (Exception e) {
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                sendAliveHeartBeat();
            } catch (Exception e) {
            }
        }, 1000, 10*1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop(){
        sendOffLineHeartBeat();
    }

    public void sendAliveHeartBeat() {

        aliveWorker.put(connectConfig.getWorkerId(), System.currentTimeMillis());
        dataSynchronizer.send(HeartBeatEnum.ALIVE.name(), aliveWorker);
    }

    public void sendOnlineHeartBeat(){

        aliveWorker.put(connectConfig.getWorkerId(), System.currentTimeMillis());
        dataSynchronizer.send(HeartBeatEnum.ONLINE_BEGIN.name(), aliveWorker);
    }

    public void sendOnlineFinishHeartBeat(){

        aliveWorker.put(connectConfig.getWorkerId(), System.currentTimeMillis());
        dataSynchronizer.send(HeartBeatEnum.ONLINE_FINISH.name(), aliveWorker);
    }

    public void sendOffLineHeartBeat(){

        Map<String, Long> offlineMap = new HashMap<>();
        offlineMap.put(connectConfig.getWorkerId(), System.currentTimeMillis());
        dataSynchronizer.send(HeartBeatEnum.OFFLINE.name(), offlineMap);
    }

    @Override
    public Map<String, Long> getAllAliveWorkers() {
        return this.aliveWorker;
    }

    @Override
    public void registerListener(WorkerStatusListener listener) {
        this.workerStatusListener.add(listener);
    }


    private boolean mergeAliveWorker(Map<String, Long> newAliveWorkerInfo) {

        removeExpiredWorker(newAliveWorkerInfo);
        boolean changed = false;
        for(String workerId : newAliveWorkerInfo.keySet()){

            Long lastAliveTime = aliveWorker.get(workerId);
            if(null == lastAliveTime){

                changed = true;
                aliveWorker.put(workerId, newAliveWorkerInfo.get(workerId));
            }else{

                if(newAliveWorkerInfo.get(workerId) > lastAliveTime){
                    changed = true;
                    aliveWorker.put(workerId, newAliveWorkerInfo.get(workerId));
                }
            }
        }

        return removeExpiredWorker(aliveWorker) && changed;
    }

    private boolean removeExpiredWorker(Map<String,Long> aliveWorker) {

        boolean changed = false;
        Iterator<String> iterator = aliveWorker.keySet().iterator();
        while(iterator.hasNext()){
            String workerId = iterator.next();
            if(aliveWorker.get(workerId) + ClusterManagementService.WORKER_TIME_OUT < System.currentTimeMillis()){
                changed = true;
                iterator.remove();
            }
        }
        return changed;
    }

    private class ClusterChangeCallback implements Callback<String, Map> {

        @Override public void onCompletion(Throwable error, String heartBeatEnum, Map result) {

            boolean changed = true;
            switch(HeartBeatEnum.valueOf(heartBeatEnum)){

                case ALIVE:
                    changed = mergeAliveWorker(result);
                    break;
                case ONLINE_BEGIN:
                    mergeAliveWorker(result);
                    changed = false;
                    sendOnlineFinishHeartBeat();
                    break;
                case ONLINE_FINISH:
                    mergeAliveWorker(result);
                    changed = true;
                    break;
                case OFFLINE:
                    for(Object key : result.keySet()){
                        String workerId = (String) key;
                        Long offlineTime = (Long)result.get(workerId);
                        Long lastOnlineTime = aliveWorker.get(workerId);
                        if(null == lastOnlineTime || lastOnlineTime > offlineTime){
                            changed = false;
                        }else{
                            changed = true;
                            aliveWorker.remove(workerId);
                        }
                    }
                    break;
                default:
                    break;
            }
            if(!changed){
                return;
            }
            for(WorkerStatusListener listener : ClusterManagementServiceImpl.this.workerStatusListener){
                listener.onWorkerChange();
            }
        }
    }

    private enum HeartBeatEnum{

        /**
         * Send when first online.
         */
        ONLINE_BEGIN,
        /**
         * Send after receive online_begin.
         */
        ONLINE_FINISH,
        /**
         * Send when offline.
         */
        OFFLINE,
        /**
         * Alive heartbeat
         */
        ALIVE
    }
}
