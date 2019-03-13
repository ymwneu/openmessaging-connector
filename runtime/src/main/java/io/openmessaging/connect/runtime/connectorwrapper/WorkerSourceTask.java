/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.openmessaging.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.Future;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.DataEntry;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.api.source.SourceTaskContext;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.crypto.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper of {@link SourceTask} for runtime.
 */
public class WorkerSourceTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    /**
     * Connector name of current task.
     */
    private String connectorName;

    /**
     * The implements of the source task.
     */
    private SourceTask sourceTask;

    /**
     * The configs of current source task.
     */
    private ConnectKeyValue taskConfig;

    /**
     * A switch for the source task.
     */
    private AtomicBoolean isStopping;

    /**
     * Used to read the position of source data source.
     */
    private PositionStorageReader positionStorageReader;

    /**
     * A OMS producer to send message to dest MQ.
     */
    private Producer producer;

    /**
     * A converter to parse source data entry to byte[].
     */
    private Converter recordConverter;

    /**
     * Current position info of the source task.
     */
    private Map<ByteBuffer, ByteBuffer> positionData = new HashMap<>();

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

    /**
     * Start a source task, and send data entry to MQ cyclically.
     */
    @Override
    public void run() {
        try {
            sourceTask.initialize(new SourceTaskContext() {
                @Override public PositionStorageReader positionStorageReader() {
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
            log.info("task stop, config:"+ JSON.toJSONString(taskConfig));
        }catch(Exception e){
            log.error("Run task failed.", e);
        }
    }

    public Map<ByteBuffer, ByteBuffer> getPositionData() {
        return positionData;
    }

    public void stop(){
        isStopping.set(true);
        producer.shutdown();
        sourceTask.stop();
    }

    /**
     * Send list of sourceDataEntries to MQ.
     * @param sourceDataEntries
     */
    private void sendRecord(Collection<SourceDataEntry> sourceDataEntries) {

        for(SourceDataEntry sourceDataEntry : sourceDataEntries){
            ByteBuffer partition = sourceDataEntry.getSourcePartition();
            ByteBuffer position = sourceDataEntry.getSourcePosition();
            sourceDataEntry.setSourcePartition(null);
            sourceDataEntry.setSourcePosition(null);
            byte[] payload = recordConverter.objectToByte(sourceDataEntry.getPayload());
            Object[] newPayload = new Object[1];
            newPayload[0] = Base64.getEncoder().encodeToString(payload);
            sourceDataEntry.setPayload(newPayload);
            Message sourceMessage = producer.createBytesMessage(sourceDataEntry.getQueueName(), JSON.toJSONString(sourceDataEntry).getBytes());
            Future<SendResult> sendResult = producer.sendAsync(sourceMessage);
            sendResult.addListener((future) -> {

                if(null != future.getThrowable()){
                    log.error("Source task send record failed.", future.getThrowable());
                }else{
                    try {
                        // send ok

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
