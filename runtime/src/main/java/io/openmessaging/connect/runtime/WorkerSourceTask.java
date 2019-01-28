package io.openmessaging.connect.runtime;

import io.openmessaging.BytesMessage;
import io.openmessaging.Future;
import io.openmessaging.FutureListener;
import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.cloudevents.CloudEvents;
import io.openmessaging.connect.runtime.cloudevents.Extension;
import io.openmessaging.connect.runtime.utils.Converter;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.sink.OMSQueue;
import io.openmessaging.connector.api.source.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
                Collection<SourceDataEntry> toSendEntries = sourceTask.poll();
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

    private void sendRecord(Collection<SourceDataEntry> entries) {

        List<CloudEvents<SourceDataEntry>> events = parseDataEntriesToCloudEvents(entries);
        for(CloudEvents<SourceDataEntry> event : events){
            SourceDataEntry dataEntry = event.getData().get();
            OMSQueue queue = dataEntry.getQueue();
            BytesMessage bytesMessage = producer.createBytesMessage(queue.getQueue(), converter.objectToByte(event));
            Future<SendResult> sendResult = producer.sendAsync(bytesMessage);
            sendResult.addListener((future) -> {
                // send ok
                Map<String, ?> sourcePartition = event.getData().get().getSourcePartition();
                Map<String, ?> sourcePosition = event.getData().get().getSourcePosition();
                positionData.put(sourcePartition, sourcePosition);
            });
        }
    }

    private List<CloudEvents<SourceDataEntry>> parseDataEntriesToCloudEvents(Collection<SourceDataEntry> entries) {

        List<CloudEvents<SourceDataEntry>> results = new ArrayList<>(entries.size());
        for(SourceDataEntry dataEntry : entries){
            CloudEvents<SourceDataEntry> cloudEvents = new CloudEvents<SourceDataEntry>() {
                @Override public String getType() {
                    return null;
                }

                @Override public String getSpecVersion() {
                    return null;
                }

                @Override public URI getSource() {
                    return null;
                }

                @Override public String getId() {
                    return null;
                }

                @Override public Optional<ZonedDateTime> getTime() {
                    return Optional.empty();
                }

                @Override public Optional<URI> getSchemaURL() {
                    return Optional.empty();
                }

                @Override public Optional<String> getContentType() {
                    return Optional.empty();
                }

                @Override public Optional<SourceDataEntry> getData() {
                    return Optional.of(dataEntry);
                }

                @Override public Optional<List<Extension>> getExtensions() {
                    return Optional.empty();
                }
            };
            results.add(cloudEvents);
        }
        return results;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public KeyValue getTaskConfig() {
        return taskConfig;
    }
}
