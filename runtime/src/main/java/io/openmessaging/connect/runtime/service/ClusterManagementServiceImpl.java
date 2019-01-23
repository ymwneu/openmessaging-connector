package io.openmessaging.connect.runtime.service;

import io.openmessaging.connect.runtime.Worker;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ClusterManagementServiceImpl implements ClusterManagementService {

    private Map<String, Long> aliveWorker;
    private DataSynchronizer<String, String> dataSynchronizer;
    private ClusterManagementService.WorkerStatusListener workerStatusListener;
    private final ScheduledExecutorService scheduledExecutorService;

    public ClusterManagementServiceImpl(Worker worker, ClusterManagementService.WorkerStatusListener workerStatusListener) {
        this.dataSynchronizer = new BrokerBasedLog<>();
        this.workerStatusListener = workerStatusListener;
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "HeartBeatScheduledThread");
            }
        });
    }

    @Override
    public void start(){

        // on worker online
        sendOnLineHeartBeat();

        dataSynchronizer.start(new ClusterChangeCallback());

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {

                    // check whether a machine is offline
                    Set<String> offLineMachine = new HashSet<>();
                    for(String workerId : aliveWorker.keySet()){
                        if((aliveWorker.get(workerId) + 30000) < System.currentTimeMillis()){
                            offLineMachine.add(workerId);
                        }
                    }
                    if(offLineMachine.size() > 0){
                        sendOffLineHeartBeat(offLineMachine);
                    }
                } catch (Exception e) {
                }
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    sendAliveHeartBeat();
                } catch (Exception e) {
                }
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop(){
        sendOffLineHeartBeat();
    }

    public void sendAliveHeartBeat() {

        // dataSynchronizer.send();
    }

    public void sendOnLineHeartBeat(){

        // dataSynchronizer
    }

    public void sendOffLineHeartBeat(){

        // dataSynchronizer.send();
    }

    public void sendOffLineHeartBeat(Set<String> workerIds){

    }

    @Override public Set<String> getAllAliveWorkers() {
        return this.aliveWorker.keySet();
    }

    private class ClusterChangeCallback implements Callback<String, String> {

        @Override public void onCompletion(Throwable error, String key, String result) {

            switch(key){

                default:
                    break;
            }
            workerStatusListener.onWorkerChange();
        }
    }
}
