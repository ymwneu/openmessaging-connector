package io.openmessaging.connect.runtime;

import io.openmessaging.Future;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connect.runtime.utils.Converter;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerSourceTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);

    private String connectorName;
    private SourceTask sourceTask;
    private KeyValue taskConfig;
    private AtomicBoolean isStopping;
    private PositionStorageReader positionStorageReader;
    private Converter converter;
    private Producer producer;
    private Map<Map<String, ?>, Map<String, ?>> positionData = new HashMap<>();

    public WorkerSourceTask(String connectorName,
                            SourceTask sourceTask,
                            KeyValue taskConfig,
                            PositionStorageReader positionStorageReader,
                            Producer producer,
                            Converter converter){
        this.connectorName = connectorName;
        this.sourceTask = sourceTask;
        this.taskConfig = taskConfig;
        this.positionStorageReader = positionStorageReader;
        this.isStopping = new AtomicBoolean(false);
        this.producer = producer;
        this.converter = converter;
    }

    @Override
    public void run() {
        try {
            sourceTask.start(taskConfig);
            System.out.println("task start");
            while (!isStopping.get()) {
                Collection<Message> toSendEntries = sourceTask.poll();
                if (null != toSendEntries && toSendEntries.size() > 0) {
                    sendRecord(toSendEntries);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public Map<Map<String, ?>, Map<String, ?>> getPositionData() {
        return positionData;
    }

    public void stop(){
        producer.shutdown();
    }

    private void sendRecord(Collection<Message> messages) {

        for(Message message : messages){

            Future<SendResult> sendResult = producer.sendAsync(message);
            sendResult.addListener((future) -> {
                // send ok
//                Map<String, ?> sourcePartition = message.getSourcePartition();
//                Map<String, ?> sourcePosition = message.getSourcePosition();
//                positionData.put(sourcePartition, sourcePosition);
            });
        }
    }

    public String getConnectorName() {
        return connectorName;
    }

    public KeyValue getTaskConfig() {
        return taskConfig;
    }
}
