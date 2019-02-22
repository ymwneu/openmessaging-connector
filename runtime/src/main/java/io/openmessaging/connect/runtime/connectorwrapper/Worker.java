package io.openmessaging.connect.runtime.connectorwrapper;

import io.netty.util.internal.ConcurrentSet;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.config.RuntimeConfigDefine;
import io.openmessaging.connect.runtime.service.TaskPositionCommitService;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connect.runtime.store.PositionStorageReaderImpl;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.data.Converter;
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

    private final String workerId;
    private Set<WorkerConnector> workingConnectors = new ConcurrentSet<>();
    private Set<WorkerSourceTask> workingTasks = new ConcurrentSet<>();
    private final ExecutorService taskExecutor;
    private PositionManagementService positionManagementService;
    private MessagingAccessPoint messagingAccessPoint;
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
        this.workerId = connectConfig.getWorkerId();
        this.taskExecutor = Executors.newCachedThreadPool();
        this.positionManagementService = positionManagementService;
        this.messagingAccessPoint = messagingAccessPoint;
        taskPositionCommitService = new TaskPositionCommitService(this);
    }

    public void start(){
        taskPositionCommitService.start();
    }

    public synchronized void startConnectors(Map<String, ConnectKeyValue> connectorConfigs) throws Exception {


        Set<WorkerConnector> stoppedConnector = new HashSet<>();
        for(WorkerConnector workerConnector : workingConnectors){
            String connectorName = workerConnector.getConnectorName();
            ConnectKeyValue keyValue = connectorConfigs.get(connectorName);
            if(null == keyValue || 0 != keyValue.getInt(RuntimeConfigDefine.CONFIG_DELETED)){
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
        Map<String, ConnectKeyValue> newConnectors = new HashMap<>();
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
            ConnectKeyValue keyValue = newConnectors.get(connectorName);
            Class clazz = Class.forName(keyValue.getString(RuntimeConfigDefine.CONNECTOR_CLASS));
            Connector connector = (Connector) clazz.newInstance();
            WorkerConnector workerConnector = new WorkerConnector(connectorName, connector, connectorConfigs.get(connectorName));
            workerConnector.start();
            this.workingConnectors.add(workerConnector);
        }
    }

    public synchronized void startTasks(Map<String, List<ConnectKeyValue>> taskConfigs) throws Exception {

        Set<WorkerSourceTask> stoppedTasks = new HashSet<>();
        for(WorkerSourceTask workerSourceTask : workingTasks){
            String connectorName = workerSourceTask.getConnectorName();
            List<ConnectKeyValue> keyValues = taskConfigs.get(connectorName);
            boolean needStop = true;
            if(null != keyValues && keyValues.size() > 0){
                for(ConnectKeyValue keyValue : keyValues){
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
        Map<String, List<ConnectKeyValue>> newTasks = new HashMap<>();
        for(String connectorName : taskConfigs.keySet()){
            for(ConnectKeyValue keyValue : taskConfigs.get(connectorName)){
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
            for(ConnectKeyValue keyValue : newTasks.get(connectorName)){
                Class taskClazz = Class.forName(keyValue.getString(RuntimeConfigDefine.CONNECTOR_CLASS));
                Task task = (Task) taskClazz.newInstance();

                Class converterClazz = Class.forName(keyValue.getString(RuntimeConfigDefine.SOURCE_RECORD_CONVERTER));
                Converter recordConverter = (Converter) converterClazz.newInstance();

                if(task instanceof SourceTask){
                    Producer producer = messagingAccessPoint.createProducer();
                    producer.startup();
                    WorkerSourceTask workerSourceTask = new WorkerSourceTask(connectorName,
                                (SourceTask) task, keyValue,
                                new PositionStorageReaderImpl(positionManagementService), recordConverter, producer);
                    this.taskExecutor.submit(workerSourceTask);
                    this.workingTasks.add(workerSourceTask);
                }
            }
        }
    }

    public void commitTaskPosition() {
        Map<byte[], byte[]> positionData = new HashMap<>();
        for(WorkerSourceTask task : workingTasks){
            positionData.putAll(task.getPositionData());
        }
        positionManagementService.putPosition(positionData);
    }

    public String getWorkerId() {
        return workerId;
    }

    public void stop() {

    }

    public Set<WorkerConnector> getWorkingConnectors() {
        return workingConnectors;
    }

    public void setWorkingConnectors(
        Set<WorkerConnector> workingConnectors) {
        this.workingConnectors = workingConnectors;
    }

    public Set<WorkerSourceTask> getWorkingTasks() {
        return workingTasks;
    }

    public void setWorkingTasks(Set<WorkerSourceTask> workingTasks) {
        this.workingTasks = workingTasks;
    }
}
