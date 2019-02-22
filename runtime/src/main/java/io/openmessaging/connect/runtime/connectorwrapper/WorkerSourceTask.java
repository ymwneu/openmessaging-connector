package io.openmessaging.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.Future;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.config.RuntimeConfigDefine;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.api.source.SourceTaskContext;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerSourceTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private String connectorName;
    private SourceTask sourceTask;
    private ConnectKeyValue taskConfig;
    private AtomicBoolean isStopping;
    private PositionStorageReader positionStorageReader;
    private Producer producer;
    private Converter recordConverter;
    private Map<byte[], byte[]> positionData = new HashMap<>();

    public WorkerSourceTask(String connectorName,
                            SourceTask sourceTask,
                            ConnectKeyValue taskConfig,
                            PositionStorageReader positionStorageReader,
                            Converter recordConverter,
                            Producer producer){
        this.connectorName = connectorName;
        this.sourceTask = sourceTask;
        this.taskConfig = taskConfig;
        this.positionStorageReader = positionStorageReader;
        this.isStopping = new AtomicBoolean(false);
        this.producer = producer;
        this.recordConverter = recordConverter;
    }

    @Override
    public void run() {
        try {
            sourceTask.initialize(new SourceTaskContext() {
                @Override public PositionStorageReader offsetStorageReader() {
                    return positionStorageReader;
                }

                @Override public KeyValue configs() {
                    return taskConfig;
                }
            });
            sourceTask.start(taskConfig);
            log.info("task start, config:"+ JSON.toJSONString(taskConfig));
            while (!isStopping.get()) {
                Collection<SourceDataEntry> toSendEntries = sourceTask.poll();
                if (null != toSendEntries && toSendEntries.size() > 0) {
                    sendRecord(toSendEntries);
                }
            }
        }catch(Exception e){
            log.error("Run task failed.", e);
        }
    }

    public Map<byte[], byte[]> getPositionData() {
        return positionData;
    }

    public void stop(){
        producer.shutdown();
        sourceTask.stop();
    }

    private void sendRecord(Collection<SourceDataEntry> sourceDataEntries) {

        for(SourceDataEntry sourceDataEntry : sourceDataEntries){

            byte[] payload = recordConverter.objectToByte(sourceDataEntry.getPayload());
            Object[] newPayload = new Object[1];
            newPayload[0] = Base64.getEncoder().encodeToString(payload);
            sourceDataEntry.setPayload(newPayload);
            Message sourceMessage = producer.createBytesMessage(taskConfig.getString(RuntimeConfigDefine.QUEUE_NAME), JSON.toJSONString(sourceDataEntry).getBytes());

            Future<SendResult> sendResult = producer.sendAsync(sourceMessage);
            sendResult.addListener((future) -> {

                if(null != future.getThrowable()){
                    log.error("Source task send record failed.", future.getThrowable());
                }else{
                    try {
                        // send ok
                        byte[] partition = sourceDataEntry.getSourcePartition();
                        byte[] position = sourceDataEntry.getSourcePosition();
                        if(null != partition && null != position){
                            positionData.put(partition, position);
                        }
                    } catch (Exception e) {
                        log.error("Source task save position info failed.", e);
                    }
                }
            });
        }
    }

    public String getConnectorName() {
        return connectorName;
    }

    public ConnectKeyValue getTaskConfig() {
        return taskConfig;
    }

    @Override
    public String toString(){

        StringBuilder sb = new StringBuilder();
        sb.append("connectorName:"+connectorName)
            .append("\nConfigs:"+ JSON.toJSONString(taskConfig));
        return sb.toString();
    }
}
