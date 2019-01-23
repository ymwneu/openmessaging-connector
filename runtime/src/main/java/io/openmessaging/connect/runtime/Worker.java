package io.openmessaging.connect.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connect.runtime.utils.BasicConverter;
import io.openmessaging.connect.runtime.utils.Converter;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.producer.Producer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class Worker {

    private Map<KeyValue, WorkerSourceTask> workingTasks;
    private final ExecutorService taskExecutor;
    private PositionManagementService positionManagementService;
    private ConnectorAccessPoint connectorAccessPoint;
    private MessagingAccessPoint messagingAccessPoint;
    private Converter converter;
    private TaskPositionCommitService taskPositionCommitService;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "WorkerScheduledThread");
        }
    });
    public Worker(PositionManagementService positionManagementService, MessagingAccessPoint messagingAccessPoint) {
        this.taskExecutor = Executors.newCachedThreadPool();
        this.positionManagementService = positionManagementService;
        this.messagingAccessPoint = messagingAccessPoint;
        this.converter = new BasicConverter();
        taskPositionCommitService = new TaskPositionCommitService(this);
    }

    public void start(){
        taskPositionCommitService.start();
    }

    public void startConnectors(Set<KeyValue> connectorConfigs){

    }

    public void startTasks(Set<KeyValue> taskConfigs){

        for(KeyValue keyValue : workingTasks.keySet()){
            if(taskConfigs.contains(keyValue)){
                continue;
            }
            this.workingTasks.get(keyValue).stop();
        }

        for(KeyValue keyValue : taskConfigs){
            Task task = connectorAccessPoint.createTask(keyValue.getString("connectorName"), keyValue);
            if(task instanceof SourceTask){
                Producer producer = messagingAccessPoint.createProducer();
                producer.startup();
                WorkerSourceTask workerSourceTask = new WorkerSourceTask((SourceTask) task, keyValue, null, producer, converter);
                this.taskExecutor.submit(workerSourceTask);
                this.workingTasks.put(keyValue, workerSourceTask);
            }
        }
    }

    public void commitTaskPosition() {
        Map<Map<String, ?>, Map<String, ?>> positionData = new HashMap<>();
        for(KeyValue keyValue : workingTasks.keySet()){
            WorkerSourceTask task = workingTasks.get(keyValue);
            positionData.putAll(task.getPositionData());
        }
        positionManagementService.putPosition(positionData);
    }
}
