package io.openmessaging.connect.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connect.runtime.utils.BasicConverter;
import io.openmessaging.connect.runtime.utils.Converter;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.producer.Producer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class Worker {

    private final String workerName;
    private Set<WorkerConnector> workingConnectors = new HashSet<>();
    private Set<WorkerSourceTask> workingTasks = new HashSet<>();
    private final ExecutorService taskExecutor;
    private PositionManagementService positionManagementService;
    private MessagingAccessPoint messagingAccessPoint;
    private Converter converter;
    private TaskPositionCommitService taskPositionCommitService;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "WorkerScheduledThread");
        }
    });

    public Worker(ConnectConfig connectConfig,
        PositionManagementService positionManagementService,
        MessagingAccessPoint messagingAccessPoint) {
        this.workerName = connectConfig.getWorkerName();
        this.taskExecutor = Executors.newCachedThreadPool();
        this.positionManagementService = positionManagementService;
        this.messagingAccessPoint = messagingAccessPoint;
        this.converter = new BasicConverter();
        taskPositionCommitService = new TaskPositionCommitService(this);
    }

    public void start(){
        taskPositionCommitService.start();
    }

    public void startConnectors(Map<String, KeyValue> connectorConfigs) throws Exception {


        Set<WorkerConnector> stoppedConnector = new HashSet<>();
        for(WorkerConnector workerConnector : workingConnectors){
            String connectorName = workerConnector.getConnectorName();
            KeyValue keyValue = connectorConfigs.get(connectorName);
            if(null == keyValue){
                workerConnector.stop();
                stoppedConnector.add(workerConnector);
            }else if(!keyValue.equals(workerConnector.getKeyValue())){
                workerConnector.reconfigure(keyValue);
            }
        }
        workingConnectors.removeAll(stoppedConnector);

        if(null == connectorConfigs || 0 == connectorConfigs.size()){
            return;
        }
        Map<String, KeyValue> newConnectors = new HashMap<>();
        for(String connectorName : connectorConfigs.keySet()){
            boolean isNewConnector = true;
            for(WorkerConnector workerConnector : workingConnectors){
                if(workerConnector.getConnectorName().equals(connectorName)){
                    isNewConnector = false;
                    break;
                }
            }
            if(isNewConnector){
                newConnectors.put(connectorName, connectorConfigs.get(connectorName));
            }
        }

        for(String connectorName : newConnectors.keySet()){
            KeyValue keyValue = newConnectors.get(connectorName);
            Class clazz = Class.forName(keyValue.getString("class"));
            Connector connector = (Connector) clazz.newInstance();
            WorkerConnector workerConnector = new WorkerConnector(connectorName, connector, connectorConfigs.get(connectorName));
            workerConnector.start();
            this.workingConnectors.add(workerConnector);
        }
    }

    public void startTasks(Map<String, List<KeyValue>> taskConfigs) throws Exception {

        Set<WorkerSourceTask> stoppedTasks = new HashSet<>();
        for(WorkerSourceTask workerSourceTask : workingTasks){
            String connectorName = workerSourceTask.getConnectorName();
            List<KeyValue> keyValues = taskConfigs.get(connectorName);
            boolean needStop = true;
            if(null != keyValues && keyValues.size() > 0){
                for(KeyValue keyValue : keyValues){
                    if(keyValue.equals(workerSourceTask.getTaskConfig())){
                        needStop = false;
                        break;
                    }
                }
            }
            if(needStop){
                workerSourceTask.stop();
                stoppedTasks.add(workerSourceTask);
            }
        }
        workingTasks.removeAll(stoppedTasks);

        if (null == taskConfigs || 0 == taskConfigs.size()){
            return;
        }
        Map<String, List<KeyValue>> newTasks = new HashMap<>();
        for(String connectorName : taskConfigs.keySet()){
            for(KeyValue keyValue : taskConfigs.get(connectorName)){
                boolean isNewTask = true;
                for(WorkerSourceTask workeringTask : workingTasks){
                    if(keyValue.equals(workeringTask.getTaskConfig())){
                        isNewTask = false;
                        break;
                    }
                }
                if(isNewTask){
                    if(!newTasks.containsKey(connectorName)){
                        newTasks.put(connectorName, new ArrayList<>());
                    }
                    newTasks.get(connectorName).add(keyValue);
                }
            }
        }

        for(String connectorName : newTasks.keySet()){
            for(KeyValue keyValue : newTasks.get(connectorName)){
                Class clazz = Class.forName(keyValue.getString("class"));
                Task task = (Task) clazz.newInstance();
                if(task instanceof SourceTask){
                    Producer producer = messagingAccessPoint.createProducer();
                    producer.startup();
                    WorkerSourceTask workerSourceTask = new WorkerSourceTask(connectorName, (SourceTask) task, keyValue, null, producer, converter);
                    this.taskExecutor.submit(workerSourceTask);
                    this.workingTasks.add(workerSourceTask);
                }
            }
        }
    }

    public void commitTaskPosition() {
        Map<Map<String, ?>, Map<String, ?>> positionData = new HashMap<>();
        for(WorkerSourceTask task : workingTasks){
            positionData.putAll(task.getPositionData());
        }
        positionManagementService.putPosition(positionData);
    }

    public String getWorkerName() {
        return workerName;
    }

}
