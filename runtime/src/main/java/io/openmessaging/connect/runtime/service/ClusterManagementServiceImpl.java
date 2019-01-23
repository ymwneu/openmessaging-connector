package io.openmessaging.connect.runtime.service;

import io.openmessaging.Message;
import io.openmessaging.connect.runtime.Worker;
import io.openmessaging.connect.runtime.store.BrokerBasedStore;
import io.openmessaging.consumer.MessageListener;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class HeartBeatServiceImpl implements HeartBeatService{

    private Map<String, Long> aliveWorker;
    private BrokerBasedStore brokerBasedStore;
    private HeartBeatService.WorkerStatusListener workerStatusListener;
    private final ScheduledExecutorService scheduledExecutorService;


    public HeartBeatServiceImpl(Worker worker, HeartBeatService.WorkerStatusListener workerStatusListener) {
        this.brokerBasedStore = new BrokerBasedStore(new HeartBeatMessageListener());
        this.workerStatusListener = workerStatusListener;
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "HeartBeatScheduledThread");
            }
        });
    }

    public void start(){
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {

                    boolean workerChanged = false;
                    for(String workerId : aliveWorker.keySet()){
                        if((aliveWorker.get(workerId) + 30000) < System.currentTimeMillis()){
                            aliveWorker.remove(workerId);
                            workerChanged = true;
                        }
                    }
                    if(workerChanged){
                        workerStatusListener.onWorkerChange();
                    }
                } catch (Exception e) {
                }
            }
        }, 1000, 20*1000, TimeUnit.MILLISECONDS);
    }

    public void sendAliveHeartBeat() {

        // producer.send();
    }

    public void sendOffLineHeartBeat(){

        // producer.send();
    }

    private class HeartBeatMessageListener implements MessageListener{

        @Override public void onReceived(Message message, Context context) {

            switch (message.sysHeaders().getString("")){
                case "heartbeat":
                    if(!aliveWorker.containsKey("worker1")){
                        aliveWorker.put("worker1", System.currentTimeMillis());
                        workerStatusListener.onWorkerChange();
                    }
                    break;
                case "offLine":
                    aliveWorker.remove("worker1");
                    break;
            }
        }
    }
}
